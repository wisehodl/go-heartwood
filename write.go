package heartwood

import (
	"context"
	"encoding/json"
	"fmt"
	roots "git.wisehodl.dev/jay/go-roots/events"
	"github.com/boltdb/bolt"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
	"sync"
	"time"
)

type WriteOptions struct {
	Expanders         ExpanderPipeline
	BoltReadBatchSize int
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
	driver neo4j.Driver, boltdb *bolt.DB,
	opts *WriteOptions,
) (WriteReport, error) {
	start := time.Now()

	if opts == nil {
		opts = &WriteOptions{}
	}

	setDefaultWriteOptions(opts)

	err := SetupBoltDB(boltdb)
	if err != nil {
		return WriteReport{}, fmt.Errorf("error setting up bolt db: %w", err)
	}

	var wg sync.WaitGroup

	// Create Event Followers
	jsonChan := make(chan string)
	eventChan := make(chan EventFollower)

	wg.Add(1)
	go createEventFollowers(&wg, jsonChan, eventChan)

	// Parse Event JSON
	parsedChan := make(chan EventFollower)
	invalidChan := make(chan EventFollower)

	wg.Add(1)
	go parseEventJSON(&wg, eventChan, parsedChan, invalidChan)

	// Collect Invalid Events
	collectedInvalidChan := make(chan []EventFollower)

	wg.Add(1)
	go collectEvents(&wg, invalidChan, collectedInvalidChan)

	// Enforce Policy Rules
	queuedChan := make(chan EventFollower)
	skippedChan := make(chan EventFollower)

	wg.Add(1)
	go enforcePolicyRules(&wg, driver, boltdb, opts.BoltReadBatchSize,
		parsedChan, queuedChan, skippedChan)

	// Collect Skipped Events
	collectedSkippedChan := make(chan []EventFollower)

	wg.Add(1)
	go collectEvents(&wg, skippedChan, collectedSkippedChan)

	// Convert Events To Subgraphs
	convertedChan := make(chan EventFollower)

	wg.Add(1)
	go convertEventsToSubgraphs(&wg, opts.Expanders, queuedChan, convertedChan)

	// Write Events To Databases
	writeResultChan := make(chan WriteResult)

	wg.Add(1)
	go writeEventsToDatabases(&wg, driver, boltdb, convertedChan, writeResultChan)

	// Send event jsons into pipeline
	go func() {
		for _, raw := range events {
			jsonChan <- raw
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
	if opts.BoltReadBatchSize == 0 {
		opts.BoltReadBatchSize = 100
	}
}

func createEventFollowers(wg *sync.WaitGroup, jsonChan chan string, eventChan chan EventFollower) {
	defer wg.Done()
	for json := range jsonChan {
		eventChan <- EventFollower{JSON: json}
	}
	close(eventChan)
}

func parseEventJSON(wg *sync.WaitGroup, inChan, parsedChan, invalidChan chan EventFollower) {
	defer wg.Done()
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
	wg *sync.WaitGroup,
	driver neo4j.Driver, boltdb *bolt.DB,
	batchSize int,
	inChan, queuedChan, skippedChan chan EventFollower,
) {
	defer wg.Done()
	var batch []EventFollower

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
	boltdb *bolt.DB,
	batch []EventFollower,
	queuedChan, skippedChan chan EventFollower,
) {
	eventIDs := make([]string, 0, len(batch))

	for _, follower := range batch {
		eventIDs = append(eventIDs, follower.ID)
	}

	existsMap := BatchCheckEventsExist(boltdb, eventIDs)

	for _, follower := range batch {
		if existsMap[follower.ID] {
			skippedChan <- follower
		} else {
			queuedChan <- follower
		}
	}
}

func convertEventsToSubgraphs(
	wg *sync.WaitGroup, expanders ExpanderPipeline,
	inChan, convertedChan chan EventFollower,
) {
	defer wg.Done()
	for follower := range inChan {
		subgraph := EventToSubgraph(follower.Event, expanders)
		follower.Subgraph = subgraph
		convertedChan <- follower
	}
	close(convertedChan)
}

func writeEventsToDatabases(
	wg *sync.WaitGroup,
	driver neo4j.Driver, boltdb *bolt.DB,
	inChan chan EventFollower,
	resultChan chan WriteResult,
) {
	defer wg.Done()
	var localWg sync.WaitGroup

	boltEventChan := make(chan EventFollower)
	graphEventChan := make(chan EventFollower)

	boltErrorChan := make(chan error)
	graphResultChan := make(chan WriteResult)

	localWg.Add(2)
	go writeEventsToBoltDB(&localWg, boltdb, boltEventChan, boltErrorChan)
	go writeEventsToGraphDB(&localWg, driver, graphEventChan, boltErrorChan, graphResultChan)

	// Fan out events to both writers
	for follower := range inChan {
		boltEventChan <- follower
		graphEventChan <- follower
	}
	close(boltEventChan)
	close(graphEventChan)

	localWg.Wait()

	graphResult := <-graphResultChan
	resultChan <- graphResult
}

func writeEventsToBoltDB(
	wg *sync.WaitGroup,
	boltdb *bolt.DB,
	inChan chan EventFollower,
	errorChan chan error,
) {
	defer wg.Done()
	var events []EventBlob

	for follower := range inChan {
		events = append(events,
			EventBlob{ID: follower.ID, JSON: follower.JSON})
	}

	err := BatchWriteEvents(boltdb, events)

	errorChan <- err
	close(errorChan)
}

func writeEventsToGraphDB(
	wg *sync.WaitGroup,
	driver neo4j.Driver,
	inChan chan EventFollower,
	boltErrorChan chan error,
	resultChan chan WriteResult,
) {
	defer wg.Done()
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

	boltErr := <-boltErrorChan
	if boltErr != nil {
		resultChan <- WriteResult{
			Error: fmt.Errorf(
				"boltdb write failed, aborting graph write: %w", boltErr,
			)}
		close(resultChan)
		return
	}

	summaries, err := MergeSubgraph(context.Background(), driver, batch)
	resultChan <- WriteResult{
		ResultSummaries: summaries,
		Error:           err,
	}
	close(resultChan)
}

func collectEvents(wg *sync.WaitGroup, inChan chan EventFollower, resultChan chan []EventFollower) {
	defer wg.Done()
	var collected []EventFollower
	for follower := range inChan {
		collected = append(collected, follower)
	}
	resultChan <- collected
	close(resultChan)
}
