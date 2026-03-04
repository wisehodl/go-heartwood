package heartwood

import (
	"context"
	"encoding/json"
	"fmt"
	roots "git.wisehodl.dev/jay/go-roots/events"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
	"sync"
	"time"
)

type WriteOptions struct {
	Expanders       ExpanderPipeline
	KVReadBatchSize int
}

type EventFollower struct {
	ID       string
	JSON     string
	Event    roots.Event
	Subgraph *EventSubgraph
	Error    error
}

type WriteResult struct {
	ResultSummaries []neo4j.ResultSummary
	Error           error
}

type WriteReport struct {
	InvalidEvents        []EventFollower
	SkippedEvents        []EventFollower
	CreatedEventCount    int
	Neo4jResultSummaries []neo4j.ResultSummary
	Duration             time.Duration
	Error                error
}

func WriteEvents(
	events []string,
	graphdb GraphDB, boltdb BoltDB,
	opts *WriteOptions,
) (WriteReport, error) {
	start := time.Now()

	if opts == nil {
		opts = &WriteOptions{}
	}

	setDefaultWriteOptions(opts)

	err := boltdb.Setup()
	if err != nil {
		return WriteReport{}, fmt.Errorf("error setting up bolt db: %w", err)
	}

	var wg sync.WaitGroup

	// Create Event Followers
	jsonChan := make(chan string, 10)
	eventChan := make(chan EventFollower, 10)

	wg.Add(1)
	go func() {
		defer wg.Done()
		createEventFollowers(jsonChan, eventChan)
	}()

	// Parse Event JSON
	parsedChan := make(chan EventFollower, 10)
	invalidChan := make(chan EventFollower, 10)

	wg.Add(1)
	go func() {
		defer wg.Done()
		parseEventJSON(eventChan, parsedChan, invalidChan)
	}()

	// Collect Invalid Events
	collectedInvalidChan := make(chan []EventFollower)

	wg.Add(1)
	go func() {
		defer wg.Done()
		collectEvents(invalidChan, collectedInvalidChan)
	}()

	// Enforce Policy Rules
	queuedChan := make(chan EventFollower, 10)
	skippedChan := make(chan EventFollower, 10)

	wg.Add(1)
	go func() {
		defer wg.Done()
		enforcePolicyRules(
			graphdb, boltdb,
			opts.KVReadBatchSize,
			parsedChan, queuedChan, skippedChan)
	}()

	// Collect Skipped Events
	collectedSkippedChan := make(chan []EventFollower)

	wg.Add(1)
	go func() {
		defer wg.Done()
		collectEvents(skippedChan, collectedSkippedChan)
	}()

	// Convert Events To Subgraphs
	convertedChan := make(chan EventFollower, 10)

	wg.Add(1)
	go func() {
		defer wg.Done()
		convertEventsToSubgraphs(opts.Expanders, queuedChan, convertedChan)
	}()

	// Write Events To Databases
	writeResultChan := make(chan WriteResult)

	wg.Add(1)
	go func() {
		defer wg.Done()
		writeEventsToDatabases(
			graphdb, boltdb,
			convertedChan, writeResultChan)
	}()

	// Send event jsons into pipeline
	go func() {
		for _, json := range events {
			jsonChan <- json
		}
		close(jsonChan)
	}()

	// Wait for pipeline to complete
	wg.Wait()

	// Collect results
	invalid := <-collectedInvalidChan
	skipped := <-collectedSkippedChan
	writeResult := <-writeResultChan

	return WriteReport{
		InvalidEvents:        invalid,
		SkippedEvents:        skipped,
		CreatedEventCount:    len(events) - len(invalid) - len(skipped),
		Neo4jResultSummaries: writeResult.ResultSummaries,
		Duration:             time.Since(start),
		Error:                writeResult.Error,
	}, writeResult.Error
}

func setDefaultWriteOptions(opts *WriteOptions) {
	if opts.Expanders == nil {
		opts.Expanders = NewExpanderPipeline(DefaultExpanders()...)
	}
	if opts.KVReadBatchSize == 0 {
		opts.KVReadBatchSize = 100
	}
}

func createEventFollowers(jsonChan chan string, eventChan chan EventFollower) {
	for json := range jsonChan {
		eventChan <- EventFollower{JSON: json}
	}
	close(eventChan)
}

func parseEventJSON(inChan, parsedChan, invalidChan chan EventFollower) {
	for follower := range inChan {
		var event roots.Event
		jsonBytes := []byte(follower.JSON)
		err := json.Unmarshal(jsonBytes, &event)
		if err != nil {
			follower.Error = err
			invalidChan <- follower
			continue
		}

		follower.ID = event.ID
		follower.Event = event
		parsedChan <- follower
	}

	close(parsedChan)
	close(invalidChan)
}

func enforcePolicyRules(
	graphdb GraphDB, boltdb BoltDB,
	batchSize int,
	inChan, queuedChan, skippedChan chan EventFollower,
) {
	batch := []EventFollower{}

	for follower := range inChan {
		batch = append(batch, follower)

		if len(batch) >= batchSize {
			processPolicyRulesBatch(boltdb, batch, queuedChan, skippedChan)
			batch = []EventFollower{}
		}
	}

	if len(batch) > 0 {
		processPolicyRulesBatch(boltdb, batch, queuedChan, skippedChan)
	}

	close(queuedChan)
	close(skippedChan)
}

func processPolicyRulesBatch(
	boltdb BoltDB,
	batch []EventFollower,
	queuedChan, skippedChan chan EventFollower,
) {
	eventIDs := []string{}

	for _, follower := range batch {
		eventIDs = append(eventIDs, follower.ID)
	}

	existsMap := boltdb.BatchCheckEventsExist(eventIDs)

	for _, follower := range batch {
		if existsMap[follower.ID] {
			skippedChan <- follower
		} else {
			queuedChan <- follower
		}
	}
}

func convertEventsToSubgraphs(
	expanders ExpanderPipeline,
	inChan, convertedChan chan EventFollower,
) {
	for follower := range inChan {
		subgraph := EventToSubgraph(follower.Event, expanders)
		follower.Subgraph = subgraph
		convertedChan <- follower
	}
	close(convertedChan)
}

func writeEventsToDatabases(
	graphdb GraphDB, boltdb BoltDB,
	inChan chan EventFollower,
	resultChan chan WriteResult,
) {
	var wg sync.WaitGroup

	kvEventChan := make(chan EventFollower, 10)
	graphEventChan := make(chan EventFollower, 10)

	kvWriteDone := make(chan struct{})

	kvErrorChan := make(chan error)
	graphResultChan := make(chan WriteResult)

	wg.Add(2)
	go func() {
		defer wg.Done()
		writeEventsToKVStore(
			boltdb,
			kvEventChan, kvWriteDone, kvErrorChan)
	}()
	go func() {
		defer wg.Done()
		writeEventsToGraphDriver(
			graphdb,
			graphEventChan, kvWriteDone, graphResultChan)
	}()

	// Fan out events to both writers
	for follower := range inChan {
		kvEventChan <- follower
		graphEventChan <- follower
	}
	close(kvEventChan)
	close(graphEventChan)

	wg.Wait()

	kvError := <-kvErrorChan
	graphResult := <-graphResultChan

	var finalErr error
	if kvError != nil && graphResult.Error != nil {
		finalErr = fmt.Errorf("kvstore: %w; graphstore: %v", kvError, graphResult.Error)
	} else if kvError != nil {
		finalErr = fmt.Errorf("kvstore: %w", kvError)
	} else if graphResult.Error != nil {
		finalErr = fmt.Errorf("graphstore: %w", graphResult.Error)
	}

	resultChan <- WriteResult{
		ResultSummaries: graphResult.ResultSummaries,
		Error:           finalErr,
	}
}

func writeEventsToKVStore(
	boltdb BoltDB,
	inChan chan EventFollower,
	done chan struct{},
	resultChan chan error,
) {
	events := []EventBlob{}

	for follower := range inChan {
		events = append(events,
			EventBlob{ID: follower.ID, JSON: follower.JSON})
	}

	err := boltdb.BatchWriteEvents(events)
	if err != nil {
		close(done)
	} else {
		done <- struct{}{}
		close(done)
	}

	resultChan <- err
	close(resultChan)
}

func writeEventsToGraphDriver(
	graphdb GraphDB,
	inChan chan EventFollower,
	start chan struct{},
	resultChan chan WriteResult,
) {
	matchKeys := NewSimpleMatchKeys()
	batch := NewBatchSubgraph(matchKeys)

	for follower := range inChan {
		for _, node := range follower.Subgraph.Nodes() {
			batch.AddNode(node)
		}
		for _, rel := range follower.Subgraph.Rels() {
			batch.AddRel(rel)
		}
	}

	_, ok := <-start
	if !ok {
		resultChan <- WriteResult{Error: fmt.Errorf("kv write failed, aborting graph write")}
		close(resultChan)
		return
	}

	summaries, err := graphdb.MergeSubgraph(context.Background(), batch)
	resultChan <- WriteResult{
		ResultSummaries: summaries,
		Error:           err,
	}
	close(resultChan)
}

func collectEvents(inChan chan EventFollower, resultChan chan []EventFollower) {
	collected := []EventFollower{}
	for follower := range inChan {
		collected = append(collected, follower)
	}
	resultChan <- collected
	close(resultChan)
}
