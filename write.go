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

type preparedWrite struct {
	Event    roots.ValidatedEvent
	Subgraph *EventSubgraph
}

type excludedEvent struct {
	Event  roots.ValidatedEvent
	Reason error
}

type WriteResult struct {
	ResultSummaries []neo4j.ResultSummary
	Error           error
}

type WriteReport struct {
	ExcludedEvents       []excludedEvent
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

	queued, excluded := enforcePolicyRules(events, boltdb, opts.BoltReadBatchSize)
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

func enforcePolicyRules(in []roots.ValidatedEvent, boltdb *bolt.DB, batchSize int) (queued []roots.ValidatedEvent, excluded []excludedEvent) {
	for i := 0; i < len(in); i += batchSize {
		end := i + batchSize
		if end > len(in) {
			end = len(in)
		}
		batch := in[i:end]

		eventIDs := make([]string, 0, len(batch))
		for _, e := range batch {
			eventIDs = append(eventIDs, e.ID())
		}

		existsMap := BatchCheckEventsExist(boltdb, eventIDs)

		for _, e := range batch {
			if existsMap[e.ID()] {
				excluded = append(excluded, excludedEvent{
					Event:  e,
					Reason: fmt.Errorf("skipped: %w", ErrDuplicate),
				})
			} else {
				queued = append(queued, e)
			}
		}
	}
	return queued, excluded
}

func convertEventsToSubgraphs(in []roots.ValidatedEvent, expanders ExpanderPipeline) []preparedWrite {
	out := make([]preparedWrite, 0, len(in))
	for _, e := range in {
		out = append(out, preparedWrite{
			Event:    e,
			Subgraph: EventToSubgraph(e, expanders),
		})
	}
	return out
}

func writeEventsToDatabases(driver neo4j.Driver, boltdb *bolt.DB, travellers []preparedWrite) WriteResult {
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

func writeEventsToBoltDB(boltdb *bolt.DB, travellers []preparedWrite) error {
	var events []EventBlob
	for _, pw := range travellers {
		j, err := json.Marshal(pw.Event)
		if err != nil {
			return fmt.Errorf("failed to serialize event %s: %w", pw.Event.ID(), err)
		}
		events = append(events, EventBlob{ID: []byte(pw.Event.ID()), JSON: j})
	}
	return BatchWriteEvents(boltdb, events)
}

func writeEventsToGraphDB(driver neo4j.Driver, travellers []preparedWrite) ([]neo4j.ResultSummary, error) {
	matchKeys := NewSimpleMatchKeys()
	batch := NewBatchSubgraph(matchKeys)

	for _, pw := range travellers {
		for _, node := range pw.Subgraph.Nodes() {
			batch.AddNode(node)
		}
		for _, rel := range pw.Subgraph.Rels() {
			batch.AddRel(rel)
		}
	}

	return MergeSubgraph(context.Background(), driver, batch)
}
