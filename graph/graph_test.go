package graph

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSimpleMatchKeys(t *testing.T) {
	matchKeys := &SimpleMatchKeys{
		Keys: map[string][]string{
			"User":  {"pubkey"},
			"Event": {"id"},
			"Tag":   {"name", "value"},
		},
	}

	t.Run("get labels", func(t *testing.T) {
		expectedLabels := []string{"Event", "Tag", "User"}
		labels := matchKeys.GetLabels()
		assert.ElementsMatch(t, expectedLabels, labels)
	})

	t.Run("get keys", func(t *testing.T) {
		expectedKeys := []string{"id"}
		keys, exists := matchKeys.GetKeys("Event")
		assert.True(t, exists)
		assert.ElementsMatch(t, expectedKeys, keys)
	})

	t.Run("unknown key", func(t *testing.T) {
		keys, exists := matchKeys.GetKeys("Unknown")
		assert.False(t, exists)
		assert.Nil(t, keys)
	})
}

func TestMatchProps(t *testing.T) {
	matchKeys := &SimpleMatchKeys{
		Keys: map[string][]string{
			"User":  {"pubkey"},
			"Event": {"id"},
		},
	}

	cases := []struct {
		name           string
		node           *Node
		wantMatchLabel string
		wantMatchProps Properties
		wantErr        bool
		wantErrText    string
	}{
		{
			name:           "matching label, all props present",
			node:           NewEventNode("abc123"),
			wantMatchLabel: "Event",
			wantMatchProps: Properties{"id": "abc123"},
		},
		{
			name:        "matching label, required prop missing",
			node:        NewNode("Event", Properties{}),
			wantErr:     true,
			wantErrText: "missing property",
		},
		{
			name:        "no recognized label",
			node:        NewNode("Tag", Properties{"name": "e", "value": "abc"}),
			wantErr:     true,
			wantErrText: "no recognized label",
		},
		{
			name: "multiple labels, one matches",
			node: &Node{
				Labels: NewSet("Event", "Unknown"),
				Props: Properties{
					"id": "abc123",
				},
			},
			wantMatchLabel: "Event",
			wantMatchProps: Properties{"id": "abc123"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			matchLabel, props, err := tc.node.MatchProps(matchKeys)
			if tc.wantErr {
				assert.Error(t, err)
				if tc.wantErrText != "" {
					assert.ErrorContains(t, err, tc.wantErrText)
				}
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.wantMatchLabel, matchLabel)
			assert.Equal(t, tc.wantMatchProps, props)
		})
	}
}
