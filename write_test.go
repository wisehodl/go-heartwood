package heartwood

import (
	roots "git.wisehodl.dev/jay/go-roots/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

// Pipeline stage tests

func TestEnforcePolicyRules(t *testing.T) {
	db := tempDB(t)
	require.NoError(t, SetupBoltDB(db))
	fx := LoadFixtures(t)

	bareEvent := fx.ValidatedEvent(t, "bare")
	genericEvent := fx.ValidatedEvent(t, "generic_tag")
	e_tag_event := fx.ValidatedEvent(t, "e_tag_valid")
	p_tag_event := fx.ValidatedEvent(t, "p_tag_valid")

	// Pre-write bare and generic_tag as existing events
	bareJSON, _ := bareEvent.MarshalJSON()
	genericJSON, _ := genericEvent.MarshalJSON()
	err := BatchWriteEvents(db, []EventBlob{
		{ID: []byte(bareEvent.ID()), JSON: bareJSON},
		{ID: []byte(genericEvent.ID()), JSON: genericJSON},
	})
	assert.NoError(t, err)

	cases := []struct {
		name         string
		input        []roots.ValidatedEvent
		wantQueued   int
		wantExcluded int
	}{
		{
			name:         "empty input",
			input:        []roots.ValidatedEvent{},
			wantQueued:   0,
			wantExcluded: 0,
		},
		{
			name:         "no duplicates",
			input:        []roots.ValidatedEvent{e_tag_event, p_tag_event},
			wantQueued:   2,
			wantExcluded: 0,
		},
		{
			name:         "some duplicates",
			input:        []roots.ValidatedEvent{bareEvent, e_tag_event},
			wantQueued:   1,
			wantExcluded: 1,
		},
		{
			name:         "all duplicates",
			input:        []roots.ValidatedEvent{bareEvent, genericEvent},
			wantQueued:   0,
			wantExcluded: 2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			queued, excluded := enforcePolicyRules(tc.input, db, 100)

			assert.Equal(t, tc.wantQueued, len(queued))
			assert.Equal(t, tc.wantExcluded, len(excluded))
			for _, ex := range excluded {
				assert.ErrorIs(t, ex.Reason, ErrDuplicate)
			}
		})
	}
}

func TestConvertEventsToSubgraphs(t *testing.T) {
	fx := LoadFixtures(t)

	cases := []struct {
		name          string
		event         roots.ValidatedEvent
		wantNodeCount int
		wantRelCount  int
	}{
		{
			name:          "event with no tags",
			event:         fx.ValidatedEvent(t, "bare"),
			wantNodeCount: 2, // event + user
			wantRelCount:  1, // signed
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			expanders := NewExpanderPipeline(DefaultExpanders()...)
			results := convertEventsToSubgraphs([]roots.ValidatedEvent{tc.event}, expanders)

			assert.Len(t, results, 1)
			assert.NotNil(t, results[0].Subgraph)
			assert.Equal(t, tc.wantNodeCount, len(results[0].Subgraph.Nodes()))
			assert.Equal(t, tc.wantRelCount, len(results[0].Subgraph.Rels()))
		})
	}
}

// Skip `writeEventsToDatabases` tests -- requires BoltDB + Neo4j
