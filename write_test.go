package heartwood

import (
	"fmt"
	roots "git.wisehodl.dev/jay/go-roots/events"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

// Test helpers

func validEventJSON(id, pubkey string) string {
	return fmt.Sprintf(`{"id":"%s","pubkey":"%s","created_at":1000,"kind":1,"content":"test","tags":[],"sig":"abc"}`, id, pubkey)
}

func invalidEventJSON() string {
	return `{invalid json`
}

// Pipeline stage tests

func TestCreateEventTravellers(t *testing.T) {
	cases := []struct {
		name     string
		input    []string
		expected []EventTraveller
	}{
		{
			name:     "empty input",
			input:    []string{},
			expected: []EventTraveller{},
		},
		{
			name:  "single json",
			input: []string{"test1"},
			expected: []EventTraveller{
				{JSON: "test1"},
			},
		},
		{
			name:  "multiple jsons",
			input: []string{"test1", "test2", "test3"},
			expected: []EventTraveller{
				{JSON: "test1"},
				{JSON: "test2"},
				{JSON: "test3"},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var wg sync.WaitGroup
			jsonChan := make(chan string)
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
		wantInvalid   int
		checkParsedID bool
		expectedID    string
	}{
		{
			name: "valid event",
			input: []EventTraveller{
				{JSON: validEventJSON("abc123", "pubkey1")},
			},
			wantParsed:    1,
			wantInvalid:   0,
			checkParsedID: true,
			expectedID:    "abc123",
		},
		{
			name: "invalid json",
			input: []EventTraveller{
				{JSON: invalidEventJSON()},
			},
			wantParsed:  0,
			wantInvalid: 1,
		},
		{
			name: "mixed batch",
			input: []EventTraveller{
				{JSON: validEventJSON("abc123", "pubkey1")},
				{JSON: invalidEventJSON()},
				{JSON: validEventJSON("def456", "pubkey2")},
			},
			wantParsed:  2,
			wantInvalid: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var wg sync.WaitGroup
			inChan := make(chan EventTraveller)
			parsedChan := make(chan EventTraveller)
			invalidChan := make(chan EventTraveller)

			wg.Add(1)
			go parseEventJSON(&wg, inChan, parsedChan, invalidChan)

			go func() {
				for _, traveller := range tc.input {
					inChan <- traveller
				}
				close(inChan)
			}()

			var parsed []EventTraveller
			var invalid []EventTraveller
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
				for f := range invalidChan {
					invalid = append(invalid, f)
				}
			}()

			collectWg.Wait()
			wg.Wait()

			assert.Equal(t, tc.wantParsed, len(parsed))
			assert.Equal(t, tc.wantInvalid, len(invalid))

			// Smoke test first parsed id
			if tc.checkParsedID && len(parsed) > 0 {
				assert.Equal(t, tc.expectedID, parsed[0].ID)
				assert.NotEmpty(t, parsed[0].Event.ID)
			}

			for _, inv := range invalid {
				assert.NotNil(t, inv.Error)
				assert.Empty(t, inv.Event.ID)
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
