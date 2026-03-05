package heartwood

import (
	roots "git.wisehodl.dev/jay/go-roots/events"
	"github.com/stretchr/testify/assert"
	"sync"
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
			var wg sync.WaitGroup
			jsonChan := make(chan []byte)
			eventChan := make(chan EventTraveller)

			wg.Add(1)
			go createEventTravellers(&wg, jsonChan, eventChan)

			go func() {
				for _, raw := range tc.input {
					jsonChan <- raw
				}
				close(jsonChan)
			}()

			var result []EventTraveller
			for traveller := range eventChan {
				result = append(result, traveller)
			}

			wg.Wait()

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
			var wg sync.WaitGroup
			inChan := make(chan EventTraveller)
			parsedChan := make(chan EventTraveller)
			rejectedChan := make(chan EventTraveller)

			wg.Add(1)
			go parseEventJSON(&wg, inChan, parsedChan, rejectedChan)

			go func() {
				for _, traveller := range tc.input {
					inChan <- traveller
				}
				close(inChan)
			}()

			var parsed []EventTraveller
			var rejected []EventTraveller
			var collectWg sync.WaitGroup

			collectWg.Add(2)

			go func() {
				defer collectWg.Done()
				for f := range parsedChan {
					parsed = append(parsed, f)
				}
			}()

			go func() {
				defer collectWg.Done()
				for f := range rejectedChan {
					rejected = append(rejected, f)
				}
			}()

			collectWg.Wait()
			wg.Wait()

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

// Skip `enforcePolicyRules` -- requires BoltDB

func TestConvertEventsToSubgraphs(t *testing.T) {
	cases := []struct {
		name          string
		event         roots.Event
		wantNodeCount int
		wantRelCount  int
	}{
		{
			name: "event with no tags",
			event: roots.Event{
				ID:        "abc123",
				PubKey:    "pubkey1",
				CreatedAt: 1000,
				Kind:      1,
				Content:   "test",
				Tags:      []roots.Tag{},
			},
			wantNodeCount: 2, // event + user
			wantRelCount:  1, // signed
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var wg sync.WaitGroup
			inChan := make(chan EventTraveller)
			convertedChan := make(chan EventTraveller)

			expanders := NewExpanderPipeline(DefaultExpanders()...)

			wg.Add(1)
			go convertEventsToSubgraphs(&wg, expanders, inChan, convertedChan)

			go func() {
				inChan <- EventTraveller{Event: tc.event}
				close(inChan)
			}()

			var result EventTraveller
			for f := range convertedChan {
				result = f
			}

			wg.Wait()

			assert.NotNil(t, result.Subgraph)
			assert.Equal(t, tc.wantNodeCount, len(result.Subgraph.Nodes()))
			assert.Equal(t, tc.wantRelCount, len(result.Subgraph.Rels()))
		})
	}
}

// Skip `writeEventsToDatabases` tests -- requires BoltDB + Neo4j

func TestCollectEvents(t *testing.T) {
	cases := []struct {
		name     string
		input    []EventTraveller
		expected int
	}{
		{
			name:     "empty channel",
			input:    []EventTraveller{},
			expected: 0,
		},
		{
			name: "multiple travellers",
			input: []EventTraveller{
				{ID: "id1"},
				{ID: "id2"},
				{ID: "id3"},
			},
			expected: 3,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var wg sync.WaitGroup
			inChan := make(chan EventTraveller)
			resultChan := make(chan []EventTraveller)

			wg.Add(1)
			go collectTravellers(&wg, inChan, resultChan)

			go func() {
				for _, f := range tc.input {
					inChan <- f
				}
				close(inChan)
			}()

			result := <-resultChan
			wg.Wait()

			assert.Equal(t, tc.expected, len(result))
		})
	}
}
