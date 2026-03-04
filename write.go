package heartwood

import (
	"fmt"
	roots "git.wisehodl.dev/jay/go-roots/events"
	"github.com/boltdb/bolt"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
	"sync"
	// "git.wisehodl.dev/jay/go-heartwood/graph"
	"time"
)

type EventFollower struct {
	ID       string
	JSON     string
	Event    roots.Event
	Subgraph EventSubgraph
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
	driver *neo4j.Driver, boltdb *bolt.DB,
) (WriteReport, error) {
	start := time.Now()

	err := setupBoltDB(boltdb)
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
		enforcePolicyRules(driver, boltdb, parsedChan, queuedChan, skippedChan)
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
		convertEventsToSubgraphs(queuedChan, convertedChan)
	}()

	// Write Events To Databases
	writeResultChan := make(chan WriteResult)

	wg.Add(1)
	go func() {
		defer wg.Done()
		writeEventsToDatabases(
			driver, boltdb,
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

func setupBoltDB(boltdb *bolt.DB) error

func createEventFollowers(jsonChan chan string, eventChan chan EventFollower)

func parseEventJSON(inChan, parsedChan, invalidChan chan EventFollower)

func enforcePolicyRules(
	driver *neo4j.Driver, boltdb *bolt.DB,
	inChan, queuedChan, skippedChan chan EventFollower)

func convertEventsToSubgraphs(inChan, convertedChan chan EventFollower)

func writeEventsToDatabases(
	driver *neo4j.Driver, boltdb *bolt.DB,
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
			kvEventChan, kvErrorChan)
	}()
	go func() {
		defer wg.Done()
		writeEventsToGraphStore(
			driver,
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
	if kvError != nil {
		close(kvWriteDone) // signal abort
		resultChan <- WriteResult{Error: kvError}
		return
	}

	// Signal graph writer to proceed
	kvWriteDone <- struct{}{}
	close(kvWriteDone)

	graphResult := <-graphResultChan
	if graphResult.Error != nil {
		resultChan <- WriteResult{Error: graphResult.Error}
		return
	}

	resultChan <- graphResult

}

func writeEventsToKVStore(
	boltdb *bolt.DB,
	inChan chan EventFollower,
	resultChan chan error,
)

func writeEventsToGraphStore(
	driver *neo4j.Driver,
	inChan chan EventFollower,
	start chan struct{},
	resultChan chan WriteResult,
)

func collectEvents(inChan chan EventFollower, resultChan chan []EventFollower)
