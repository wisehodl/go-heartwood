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

type EventTraveller struct {
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
	InvalidEvents        []EventTraveller
	SkippedEvents        []EventTraveller
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

	// Create Event Travellers
	jsonChan := make(chan string)
	eventChan := make(chan EventTraveller)

	wg.Add(1)
	go createEventTravellers(&wg, jsonChan, eventChan)

	// Parse Event JSON
	parsedChan := make(chan EventTraveller)
	invalidChan := make(chan EventTraveller)

	wg.Add(1)
	go parseEventJSON(&wg, eventChan, parsedChan, invalidChan)

	// Collect Invalid Events
	collectedInvalidChan := make(chan []EventTraveller)

	wg.Add(1)
	go collectTravellers(&wg, invalidChan, collectedInvalidChan)

	// Enforce Policy Rules
	queuedChan := make(chan EventTraveller)
	skippedChan := make(chan EventTraveller)

	wg.Add(1)
	go enforcePolicyRules(&wg, driver, boltdb, opts.BoltReadBatchSize,
		parsedChan, queuedChan, skippedChan)

	// Collect Skipped Events
	collectedSkippedChan := make(chan []EventTraveller)

	wg.Add(1)
	go collectTravellers(&wg, skippedChan, collectedSkippedChan)

	// Convert Events To Subgraphs
	convertedChan := make(chan EventTraveller)

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

func createEventTravellers(wg *sync.WaitGroup, jsonChan chan string, eventChan chan EventTraveller) {
	defer wg.Done()
	for json := range jsonChan {
		eventChan <- EventTraveller{JSON: json}
	}
	close(eventChan)
}

func parseEventJSON(wg *sync.WaitGroup, inChan, parsedChan, invalidChan chan EventTraveller) {
	defer wg.Done()
	for traveller := range inChan {
		var event roots.Event
		jsonBytes := []byte(traveller.JSON)
		err := json.Unmarshal(jsonBytes, &event)
		if err != nil {
			traveller.Error = err
			invalidChan <- traveller
			continue
		}

		traveller.ID = event.ID
		traveller.Event = event
		parsedChan <- traveller
	}

	close(parsedChan)
	close(invalidChan)
}

func enforcePolicyRules(
	wg *sync.WaitGroup,
	driver neo4j.Driver, boltdb *bolt.DB,
	batchSize int,
	inChan, queuedChan, skippedChan chan EventTraveller,
) {
	defer wg.Done()
	var batch []EventTraveller

	for traveller := range inChan {
		batch = append(batch, traveller)

		if len(batch) >= batchSize {
			processPolicyRulesBatch(boltdb, batch, queuedChan, skippedChan)
			batch = []EventTraveller{}
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
	batch []EventTraveller,
	queuedChan, skippedChan chan EventTraveller,
) {
	eventIDs := make([]string, 0, len(batch))

	for _, traveller := range batch {
		eventIDs = append(eventIDs, traveller.ID)
	}

	existsMap := BatchCheckEventsExist(boltdb, eventIDs)

	for _, traveller := range batch {
		if existsMap[traveller.ID] {
			skippedChan <- traveller
		} else {
			queuedChan <- traveller
		}
	}
}

func convertEventsToSubgraphs(
	wg *sync.WaitGroup, expanders ExpanderPipeline,
	inChan, convertedChan chan EventTraveller,
) {
	defer wg.Done()
	for traveller := range inChan {
		subgraph := EventToSubgraph(traveller.Event, expanders)
		traveller.Subgraph = subgraph
		convertedChan <- traveller
	}
	close(convertedChan)
}

func writeEventsToDatabases(
	wg *sync.WaitGroup,
	driver neo4j.Driver, boltdb *bolt.DB,
	inChan chan EventTraveller,
	resultChan chan WriteResult,
) {
	defer wg.Done()
	var localWg sync.WaitGroup

	boltEventChan := make(chan EventTraveller)
	graphEventChan := make(chan EventTraveller)

	boltErrorChan := make(chan error)
	graphResultChan := make(chan WriteResult)

	localWg.Add(2)
	go writeEventsToBoltDB(&localWg, boltdb, boltEventChan, boltErrorChan)
	go writeEventsToGraphDB(&localWg, driver, graphEventChan, boltErrorChan, graphResultChan)

	// Fan out events to both writers
	for traveller := range inChan {
		boltEventChan <- traveller
		graphEventChan <- traveller
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
	inChan chan EventTraveller,
	errorChan chan error,
) {
	defer wg.Done()
	var events []EventBlob

	for traveller := range inChan {
		events = append(events,
			EventBlob{ID: traveller.ID, JSON: traveller.JSON})
	}

	err := BatchWriteEvents(boltdb, events)

	errorChan <- err
	close(errorChan)
}

func writeEventsToGraphDB(
	wg *sync.WaitGroup,
	driver neo4j.Driver,
	inChan chan EventTraveller,
	boltErrorChan chan error,
	resultChan chan WriteResult,
) {
	defer wg.Done()
	matchKeys := NewSimpleMatchKeys()
	batch := NewBatchSubgraph(matchKeys)

	for traveller := range inChan {
		for _, node := range traveller.Subgraph.Nodes() {
			batch.AddNode(node)
		}
		for _, rel := range traveller.Subgraph.Rels() {
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

func collectTravellers(wg *sync.WaitGroup, inChan chan EventTraveller, resultChan chan []EventTraveller) {
	defer wg.Done()
	var collected []EventTraveller
	for traveller := range inChan {
		collected = append(collected, traveller)
	}
	resultChan <- collected
	close(resultChan)
}
