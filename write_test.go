package heartwood

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

// Pipeline stage tests

func TestEnforcePolicyRules(t *testing.T) {
	db := tempDB(t)
	require.NoError(t, SetupBoltDB(db))
	fx := LoadFixtures(t)

	// Pre-write bare and generic_tag as existing events
	bareJSON, _ := fx.ValidatedEvent(t, "bare").MarshalJSON()
	genericJSON, _ := fx.ValidatedEvent(t, "generic_tag").MarshalJSON()
	bareID := fx.ValidatedEvent(t, "bare").ID()
	genericID := fx.ValidatedEvent(t, "generic_tag").ID()

	err := BatchWriteEvents(db, []EventBlob{
		{ID: []byte(bareID), JSON: bareJSON},
		{ID: []byte(genericID), JSON: genericJSON},
	})
	assert.NoError(t, err)

	e_tag_id := fx.ValidatedEvent(t, "e_tag_valid").ID()
	p_tag_id := fx.ValidatedEvent(t, "p_tag_valid").ID()

	cases := []struct {
		name         string
		input        []EventTraveller
		wantQueued   int
		wantExcluded int
	}{
		{
			name:         "empty input",
			input:        []EventTraveller{},
			wantQueued:   0,
			wantExcluded: 0,
		},
		{
			name: "no duplicates",
			input: []EventTraveller{
				{ID: e_tag_id},
				{ID: p_tag_id},
			},
			wantQueued:   2,
			wantExcluded: 0,
		},
		{
			name: "some duplicates",
			input: []EventTraveller{
				{ID: bareID},
				{ID: e_tag_id},
			},
			wantQueued:   1,
			wantExcluded: 1,
		},
		{
			name: "all duplicates",
			input: []EventTraveller{
				{ID: bareID},
				{ID: genericID},
			},
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
				assert.ErrorIs(t, ex.Error, ErrDuplicate)
			}
		})
	}
}

func TestConvertEventsToSubgraphs(t *testing.T) {
	fx := LoadFixtures(t)

	cases := []struct {
		name          string
		traveller     EventTraveller
		wantNodeCount int
		wantRelCount  int
	}{
		{
			name:          "event with no tags",
			traveller:     EventTraveller{Event: fx.ValidatedEvent(t, "bare")},
			wantNodeCount: 2, // event + user
			wantRelCount:  1, // signed
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			expanders := NewExpanderPipeline(DefaultExpanders()...)
			results := convertEventsToSubgraphs([]EventTraveller{tc.traveller}, expanders)

			assert.Len(t, results, 1)
			assert.NotNil(t, results[0].Subgraph)
			assert.Equal(t, tc.wantNodeCount, len(results[0].Subgraph.Nodes()))
			assert.Equal(t, tc.wantRelCount, len(results[0].Subgraph.Rels()))
		})
	}
}

// Skip `writeEventsToDatabases` tests -- requires BoltDB + Neo4j
