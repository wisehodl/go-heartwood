package heartwood

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMatchKeys(t *testing.T) {
	matchKeys := &MatchKeys{
		keys: map[string][]string{
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

func TestNodeSortKey(t *testing.T) {
	matchLabel := "Event"
	labels := []string{"Event", "AddressableEvent"}

	// labels should be sorted by key generator
	expectedKey := "Event:AddressableEvent,Event"

	// Test Serialization
	sortKey := createNodeSortKey(matchLabel, labels)
	assert.Equal(t, expectedKey, sortKey)

	// Test Deserialization
	returnedMatchLabel, returnedLabels, err := DeserializeNodeKey(sortKey)
	assert.NoError(t, err)
	assert.Equal(t, matchLabel, returnedMatchLabel)
	assert.ElementsMatch(t, labels, returnedLabels)
}

func TestRelSortKey(t *testing.T) {
	rtype, startLabel, endLabel := "SIGNED", "User", "Event"
	expectedKey := "SIGNED,User,Event"

	// Test Serialization
	sortKey := createRelSortKey(rtype, startLabel, endLabel)
	assert.Equal(t, expectedKey, sortKey)

	// Test Deserialization
	returnedRtype, returnedStartLabel, returnedEndLabel, err := DeserializeRelKey(sortKey)
	assert.NoError(t, err)
	assert.Equal(t, rtype, returnedRtype)
	assert.Equal(t, startLabel, returnedStartLabel)
	assert.Equal(t, endLabel, returnedEndLabel)
}

func TestMatchProps(t *testing.T) {
	matchKeys := &MatchKeys{
		keys: map[string][]string{
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

func TestStructuredSubgraphAddNode(t *testing.T) {
	matchKeys := NewMatchKeys()
	subgraph := NewStructuredSubgraph(matchKeys)
	node := NewEventNode("abc123")

	err := subgraph.AddNode(node)

	assert.NoError(t, err)
	assert.Equal(t, 1, subgraph.NodeCount())
	assert.Equal(t, []*Node{node}, subgraph.GetNodes("Event:Event"))
}

func TestStructuredSubgraphAddNodeInvalid(t *testing.T) {
	matchKeys := NewMatchKeys()
	subgraph := NewStructuredSubgraph(matchKeys)
	node := NewNode("Event", Properties{})

	err := subgraph.AddNode(node)

	assert.ErrorContains(t, err, "invalid node: missing property id")
	assert.Equal(t, 0, subgraph.NodeCount())
}

func TestStructuredSubgraphAddRel(t *testing.T) {
	matchKeys := NewMatchKeys()
	subgraph := NewStructuredSubgraph(matchKeys)

	userNode := NewUserNode("pubkey1")
	eventNode := NewEventNode("abc123")
	rel, _ := NewSignedRel(userNode, eventNode, nil)

	err := subgraph.AddRel(rel)

	assert.NoError(t, err)
	assert.Equal(t, 1, subgraph.RelCount())
	assert.Equal(t, []*Relationship{rel}, subgraph.GetRels("SIGNED,User,Event"))

}
