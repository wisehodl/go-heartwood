package heartwood

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

// Test helpers

func validEventJSON() []byte {
	return []byte(`{"id":"c7a702e6158744ca03508bbb4c90f9dbb0d6e88fefbfaa511d5ab24b4e3c48ad","pubkey":"cfa87f35acbde29ba1ab3ee42de527b2cad33ac487e80cf2d6405ea0042c8fef","created_at":1760740551,"kind":1,"tags":[],"content":"hello world","sig":"83b71e15649c9e9da362c175f988c36404cabf357a976d869102a74451cfb8af486f6088b5631033b4927bd46cad7a0d90d7f624aefc0ac260364aa65c36071a"}`)
}

func invalidEventJSON() []byte {
	return []byte(`{"id":"abc123","pubkey":"xyz789","created_at":1000,"kind":1,"content":"test","tags":[],"sig":"abc"}`)
}

func malformedEventJSON() []byte {
	return []byte(`{malformed json`)
}

// Pipeline stage tests

func TestCreateEventTravellers(t *testing.T) {
	cases := []struct {
		name     string
		input    [][]byte
		expected []EventTraveller
	}{
		{
			name:     "empty input",
			input:    [][]byte{},
			expected: []EventTraveller{},
		},
		{
			name:  "single json",
			input: [][]byte{[]byte("test1")},
			expected: []EventTraveller{
				{JSON: []byte("test1")},
			},
		},
		{
			name:  "multiple jsons",
			input: [][]byte{[]byte("test1"), []byte("test2"), []byte("test3")},
			expected: []EventTraveller{
				{JSON: []byte("test1")},
				{JSON: []byte("test2")},
				{JSON: []byte("test3")},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := createEventTravellers(tc.input)

			assert.Equal(t, len(tc.expected), len(result))
			for i := range tc.expected {
				assert.Equal(t, tc.expected[i].JSON, result[i].JSON)
			}
		})
	}
}

func TestParseEventJSON(t *testing.T) {
	cases := []struct {
		name          string
		input         []EventTraveller
		wantParsed    int
		wantRejected  int
		checkParsedID bool
		expectedID    string
		wantErrorText string
	}{
		{
			name: "valid event",
			input: []EventTraveller{
				{JSON: validEventJSON()},
			},
			wantParsed:    1,
			wantRejected:  0,
			checkParsedID: true,
			expectedID:    "c7a702e6158744ca03508bbb4c90f9dbb0d6e88fefbfaa511d5ab24b4e3c48ad",
		},
		{
			name: "invalid event",
			input: []EventTraveller{
				{JSON: invalidEventJSON()},
			},
			wantParsed:    0,
			wantRejected:  1,
			wantErrorText: "rejected: invalid event",
		},
		{
			name: "malformed json",
			input: []EventTraveller{
				{JSON: malformedEventJSON()},
			},
			wantParsed:    0,
			wantRejected:  1,
			wantErrorText: "rejected: unrecognized event format",
		},
		{
			name: "mixed batch",
			input: []EventTraveller{
				{JSON: invalidEventJSON()},
				{JSON: malformedEventJSON()},
				{JSON: validEventJSON()},
			},
			wantParsed:   1,
			wantRejected: 2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, rejected := parseEventJSON(tc.input)

			assert.Equal(t, tc.wantParsed, len(parsed))
			assert.Equal(t, tc.wantRejected, len(rejected))

			// Smoke test first parsed id
			if tc.checkParsedID && len(parsed) > 0 {
				assert.Equal(t, tc.expectedID, parsed[0].ID)
				assert.NotEmpty(t, parsed[0].Event.ID)
			}

			// Check error text on first rejected event
			if tc.wantErrorText != "" {
				assert.ErrorContains(t, rejected[0].Error, tc.wantErrorText)
			}

			for _, reject := range rejected {
				assert.NotNil(t, reject.Error)
				assert.Empty(t, reject.Event.ID)
			}
		})
	}
}

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
			traveller:     EventTraveller{Event: fx.ValidatedEvent(t, "bare").Event()},
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
