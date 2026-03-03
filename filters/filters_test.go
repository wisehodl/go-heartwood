package filters

import (
	"encoding/json"
	roots "git.wisehodl.dev/jay/go-roots/filters"
	"github.com/stretchr/testify/assert"
	"testing"
)

// Helpers

func intPtr(i int) *int {
	return &i
}

func expectEqualHeartwoodFilters(t *testing.T, got, want HeartwoodFilter) {
	t.Helper()
	assert.Equal(t, want.Legacy.IDs, got.Legacy.IDs)
	assert.Equal(t, want.Legacy.Authors, got.Legacy.Authors)
	assert.Equal(t, want.Legacy.Kinds, got.Legacy.Kinds)
	assert.Equal(t, want.Legacy.Since, got.Legacy.Since)
	assert.Equal(t, want.Legacy.Until, got.Legacy.Until)
	assert.Equal(t, want.Legacy.Limit, got.Legacy.Limit)
	assert.Equal(t, want.Legacy.Tags, got.Legacy.Tags)
	assert.Equal(t, len(want.Graph), len(got.Graph))
	for i := range want.Graph {
		expectEqualGraphFilters(t, got.Graph[i], want.Graph[i])
	}
}

func expectEqualGraphFilters(t *testing.T, got, want GraphFilter) {
	t.Helper()
	assert.Equal(t, want.IDs, got.IDs)
	assert.Equal(t, want.Authors, got.Authors)
	assert.Equal(t, want.Kinds, got.Kinds)
	assert.Equal(t, want.Since, got.Since)
	assert.Equal(t, want.Until, got.Until)
	assert.Equal(t, want.Limit, got.Limit)
	assert.Equal(t, want.Tags, got.Tags)
	assert.Equal(t, want.Distance, got.Distance)
	assert.Equal(t, len(want.Graph), len(got.Graph))
	for i := range want.Graph {
		expectEqualGraphFilters(t, got.Graph[i], want.Graph[i])
	}
	assert.Equal(t, want.Extensions, got.Extensions)
}

// Tests

func TestMarshalJSON(t *testing.T) {
	cases := []struct {
		name     string
		filter   HeartwoodFilter
		expected string
	}{
		{
			name:     "empty filter",
			filter:   HeartwoodFilter{},
			expected: `{}`,
		},
		{
			name: "legacy fields only",
			filter: HeartwoodFilter{
				Legacy: roots.Filter{
					IDs:   []string{"abc"},
					Kinds: []int{1},
					Since: intPtr(1000),
				},
			},
			expected: `{"ids":["abc"],"kinds":[1],"since":1000}`,
		},
		{
			name: "empty graph field",
			filter: HeartwoodFilter{
				Graph: []GraphFilter{},
			},
			expected: `{"graph":[]}`,
		},
		{
			name: "graph field only",
			filter: HeartwoodFilter{
				Graph: []GraphFilter{
					{Kinds: []json.RawMessage{json.RawMessage(`1`)}},
				},
			},
			expected: `{"graph":[{"kinds":[1]}]}`,
		},
		{
			name: "legacy and graph present",
			filter: HeartwoodFilter{
				Legacy: roots.Filter{
					IDs: []string{"abc"},
				},
				Graph: []GraphFilter{
					{Kinds: []json.RawMessage{json.RawMessage(`1`)}},
				},
			},
			expected: `{"ids":["abc"],"graph":[{"kinds":[1]}]}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := MarshalJSON(tc.filter)
			assert.NoError(t, err)

			var expectedMap, actualMap map[string]interface{}
			assert.NoError(t, json.Unmarshal([]byte(tc.expected), &expectedMap))
			assert.NoError(t, json.Unmarshal(result, &actualMap))
			assert.Equal(t, expectedMap, actualMap)
		})
	}

}

func TestUnmarshalJSON(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		expected HeartwoodFilter
	}{
		{
			name:     "empty object",
			input:    `{}`,
			expected: HeartwoodFilter{},
		},
		{
			name:  "legacy fields only",
			input: `{"ids":["abc"],"kinds":[1],"since":1000}`,
			expected: HeartwoodFilter{
				Legacy: roots.Filter{
					IDs:   []string{"abc"},
					Kinds: []int{1},
					Since: intPtr(1000),
				},
			},
		},
		{
			name:  "empty graph field",
			input: `{"graph":[]}`,
			expected: HeartwoodFilter{
				Graph: []GraphFilter{},
			},
		},
		{
			name:  "graph field only",
			input: `{"graph":[{"kinds":[1]}]}`,
			expected: HeartwoodFilter{
				Graph: []GraphFilter{
					{Kinds: []json.RawMessage{json.RawMessage(`1`)}},
				},
			},
		},
		{
			name:  "legacy and graph present",
			input: `{"ids":["abc"],"graph":[{"kinds":[1]}]}`,
			expected: HeartwoodFilter{
				Legacy: roots.Filter{
					IDs: []string{"abc"},
				},
				Graph: []GraphFilter{
					{Kinds: []json.RawMessage{json.RawMessage(`1`)}},
				},
			},
		},
		{
			name:  "graph is removed from legacy extensions",
			input: `{"ids":["abc"],"graph":[{"kinds":[1]}],"search":"bitcoin"}`,
			expected: HeartwoodFilter{
				Legacy: roots.Filter{
					IDs: []string{"abc"},
					Extensions: map[string]json.RawMessage{
						"search": json.RawMessage(`"bitcoin"`),
					},
				},
				Graph: []GraphFilter{
					{Kinds: []json.RawMessage{json.RawMessage(`1`)}},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var result HeartwoodFilter
			err := UnmarshalJSON([]byte(tc.input), &result)
			assert.NoError(t, err)
			expectEqualHeartwoodFilters(t, result, tc.expected)

			// Ensure graph extension was popped from legacy filter
			assert.Nil(t, result.Legacy.Extensions["graph"])
		})
	}
}

func TestMarshalGraphJSON(t *testing.T) {
	cases := []struct {
		name     string
		filter   GraphFilter
		expected string
	}{
		{
			name:     "empty filter",
			filter:   GraphFilter{},
			expected: `{}`,
		},
		{
			name: "standard fields",
			filter: GraphFilter{
				IDs:     []string{"abc"},
				Authors: []string{"def"},
				Kinds:   []json.RawMessage{json.RawMessage(`1`)},
				Since:   json.RawMessage(`1000`),
				Until:   []byte("2000"),
				Limit:   intPtr(10),
			},
			expected: `{"ids":["abc"],"authors":["def"],"kinds":[1],"since":1000,"until":2000,"limit":10}`,
		},
		{
			name: "tag field",
			filter: GraphFilter{
				Tags: roots.TagFilters{"e": {"event1"}},
			},
			expected: `{"#e":["event1"]}`,
		},
		{
			name: "distance present",
			filter: GraphFilter{
				Distance: &Distance{Min: 1, Max: 10},
			},
			expected: `{"distance":{"min":1,"max":10}}`,
		},
		{
			name:     "distance absent",
			filter:   GraphFilter{Distance: nil},
			expected: `{}`,
		},
		{
			name: "empty graph",
			filter: GraphFilter{
				Kinds: []json.RawMessage{json.RawMessage(`1`)},
				Graph: []GraphFilter{},
			},
			expected: `{"kinds":[1],"graph":[]}`,
		},
		{
			name: "nested graph",
			filter: GraphFilter{
				Kinds: []json.RawMessage{json.RawMessage(`1`)},
				Graph: []GraphFilter{
					{Kinds: []json.RawMessage{json.RawMessage(`7`)}},
				},
			},
			expected: `{"kinds":[1],"graph":[{"kinds":[7]}]}`,
		},
		{
			name: "extensions",
			filter: GraphFilter{
				Extensions: roots.FilterExtensions{
					"search": json.RawMessage(`"abc"`),
				},
			},
			expected: `{"search":"abc"}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := MarshalGraphJSON(tc.filter)
			assert.NoError(t, err)

			var expectedMap, actualMap map[string]interface{}
			assert.NoError(t, json.Unmarshal([]byte(tc.expected), &expectedMap))
			assert.NoError(t, json.Unmarshal(result, &actualMap))
			assert.Equal(t, expectedMap, actualMap)
		})
	}
}

func TestUnmarshalGraphJSON(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		expected GraphFilter
	}{
		{
			name:     "empty object",
			input:    `{}`,
			expected: GraphFilter{},
		},
		{
			name:  "standard fields",
			input: `{"ids":["abc"],"authors":["def"],"kinds":[1],"since":1000,"until":2000,"limit":10}`,
			expected: GraphFilter{
				IDs:     []string{"abc"},
				Authors: []string{"def"},
				Kinds:   []json.RawMessage{json.RawMessage(`1`)},
				Since:   json.RawMessage(`1000`),
				Until:   json.RawMessage(`2000`),
				Limit:   intPtr(10),
			},
		},
		{
			name:  "tag field",
			input: `{"#e":["event1"]}`,
			expected: GraphFilter{
				Tags: roots.TagFilters{"e": {"event1"}},
			},
		},
		{
			name:  "distance present",
			input: `{"distance":{"min":1,"max":10}}`,
			expected: GraphFilter{
				Distance: &Distance{Min: 1, Max: 10},
			},
		},
		{
			name:     "distance absent",
			input:    `{}`,
			expected: GraphFilter{Distance: nil},
		},
		{
			name:  "empty graph",
			input: `{"kinds":[1],"graph":[]}`,
			expected: GraphFilter{
				Kinds: []json.RawMessage{json.RawMessage(`1`)},
				Graph: []GraphFilter{},
			},
		},
		{
			name:  "nested graph",
			input: `{"kinds":[1],"graph":[{"kinds":[7]}]}`,
			expected: GraphFilter{
				Kinds: []json.RawMessage{json.RawMessage(`1`)},
				Graph: []GraphFilter{
					{Kinds: []json.RawMessage{json.RawMessage(`7`)}},
				},
			},
		},
		{
			name:  "fully populated",
			input: `{"ids":["abc"],"authors":["def"],"kinds":[1],"since":1000,"until":2000,"limit":10,"#e":["event1"],"distance":{"min":1,"max":5},"graph":[{"kinds":[7]}]}`,
			expected: GraphFilter{
				IDs:      []string{"abc"},
				Authors:  []string{"def"},
				Kinds:    []json.RawMessage{json.RawMessage(`1`)},
				Since:    json.RawMessage(`1000`),
				Until:    json.RawMessage(`2000`),
				Limit:    intPtr(10),
				Tags:     roots.TagFilters{"e": {"event1"}},
				Distance: &Distance{Min: 1, Max: 5},
				Graph: []GraphFilter{
					{Kinds: []json.RawMessage{json.RawMessage(`7`)}},
				},
			},
		},
		{
			name:  "unknown fields routed to extensions",
			input: `{"search":"abc"}`,
			expected: GraphFilter{
				Extensions: roots.FilterExtensions{
					"search": json.RawMessage(`"abc"`),
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var result GraphFilter
			err := UnmarshalGraphJSON([]byte(tc.input), &result)
			assert.NoError(t, err)
			expectEqualGraphFilters(t, result, tc.expected)
		})
	}
}
