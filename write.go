package heartwood

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	roots "git.wisehodl.dev/jay/go-roots/events"
	"github.com/boltdb/bolt"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
	"time"
)

type WriteOptions struct {
	Expanders         ExpanderPipeline
	BoltReadBatchSize int
}

type EventTraveller struct {
	ID       string
	Event    roots.ValidatedEvent
	Subgraph *EventSubgraph
	Error    error
}

type WriteResult struct {
	ResultSummaries []neo4j.ResultSummary
	Error           error
}

type WriteReport struct {
	ExcludedEvents       []EventTraveller
	CreatedEventCount    int
	Neo4jResultSummaries []neo4j.ResultSummary
	Duration             time.Duration
	Error                error
}

var ErrDuplicate = errors.New("event already exists")

func WriteEvents(
	events []roots.ValidatedEvent,
	driver neo4j.Driver, boltdb *bolt.DB,
	opts *WriteOptions,
) WriteReport {
	start := time.Now()

	if opts == nil {
		opts = &WriteOptions{}
	}

	setDefaultWriteOptions(opts)

	err := SetupBoltDB(boltdb)
	if err != nil {
		return WriteReport{Error: fmt.Errorf("error setting up bolt db: %w", err)}
	}

	travellers := make([]EventTraveller, 0, len(events))
	for _, e := range events {
		travellers = append(travellers, EventTraveller{ID: e.ID(), Event: e})
	}

	queued, excluded := enforcePolicyRules(travellers, boltdb, opts.BoltReadBatchSize)
	converted := convertEventsToSubgraphs(queued, opts.Expanders)
	writeResult := writeEventsToDatabases(driver, boltdb, converted)

	return WriteReport{
		ExcludedEvents:       excluded,
		CreatedEventCount:    len(events) - len(excluded),
		Neo4jResultSummaries: writeResult.ResultSummaries,
		Duration:             time.Since(start),
		Error:                writeResult.Error,
	}
}

func setDefaultWriteOptions(opts *WriteOptions) {
	if opts.Expanders == nil {
		opts.Expanders = NewExpanderPipeline(DefaultExpanders()...)
	}
	if opts.BoltReadBatchSize == 0 {
		opts.BoltReadBatchSize = 100
	}
}

func enforcePolicyRules(in []EventTraveller, boltdb *bolt.DB, batchSize int) (queued []EventTraveller, excluded []EventTraveller) {
	for i := 0; i < len(in); i += batchSize {
		end := i + batchSize
		if end > len(in) {
			end = len(in)
		}
		batch := in[i:end]

		eventIDs := make([]string, 0, len(batch))
		for _, traveller := range batch {
			eventIDs = append(eventIDs, traveller.ID)
		}

		existsMap := BatchCheckEventsExist(boltdb, eventIDs)

		for _, traveller := range batch {
			if existsMap[traveller.ID] {
				traveller.Error = fmt.Errorf("skipped: %w", ErrDuplicate)
				excluded = append(excluded, traveller)
			} else {
				queued = append(queued, traveller)
			}
		}
	}
	return queued, excluded
}

func convertEventsToSubgraphs(in []EventTraveller, expanders ExpanderPipeline) []EventTraveller {
	for i, traveller := range in {
		in[i].Subgraph = EventToSubgraph(traveller.Event, expanders)
	}
	return in
}

func writeEventsToDatabases(driver neo4j.Driver, boltdb *bolt.DB, travellers []EventTraveller) WriteResult {
	boltErr := writeEventsToBoltDB(boltdb, travellers)
	if boltErr != nil {
		return WriteResult{
			Error: fmt.Errorf("boltdb write failed, aborting graph write: %w", boltErr),
		}
	}

	summaries, err := writeEventsToGraphDB(driver, travellers)
	return WriteResult{
		ResultSummaries: summaries,
		Error:           err,
	}
}

func writeEventsToBoltDB(boltdb *bolt.DB, travellers []EventTraveller) error {
	var events []EventBlob
	for _, traveller := range travellers {
		j, err := json.Marshal(traveller.Event)
		if err != nil {
			return fmt.Errorf("failed to serialize event %s: %w", traveller.ID, err)
		}
		events = append(events, EventBlob{ID: []byte(traveller.ID), JSON: j})
	}
	return BatchWriteEvents(boltdb, events)
}

func writeEventsToGraphDB(driver neo4j.Driver, travellers []EventTraveller) ([]neo4j.ResultSummary, error) {
	matchKeys := NewSimpleMatchKeys()
	batch := NewBatchSubgraph(matchKeys)

	for _, traveller := range travellers {
		for _, node := range traveller.Subgraph.Nodes() {
			batch.AddNode(node)
		}
		for _, rel := range traveller.Subgraph.Rels() {
			batch.AddRel(rel)
		}
	}

	return MergeSubgraph(context.Background(), driver, batch)
}
