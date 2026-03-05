package heartwood

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNodeBatchKey(t *testing.T) {
	matchLabel := "Event"
	sortedLabels := []string{"AddressableEvent", "Event"}
	expectedKey := "Event:AddressableEvent,Event"

	// Test Serialization
	// labels are expected to be pre-sorted
	batchKey := createNodeBatchKey(matchLabel, sortedLabels)
	assert.Equal(t, expectedKey, batchKey)

	// Test Deserialization
	returnedMatchLabel, returnedLabels, err := deserializeNodeBatchKey(batchKey)
	assert.NoError(t, err)
	assert.Equal(t, matchLabel, returnedMatchLabel)
	assert.ElementsMatch(t, sortedLabels, returnedLabels)
}

func TestRelBatchKey(t *testing.T) {
	rtype, startLabel, endLabel := "SIGNED", "User", "Event"
	expectedKey := "SIGNED,User,Event"

	// Test Serialization
	batchKey := createRelBatchKey(rtype, startLabel, endLabel)
	assert.Equal(t, expectedKey, batchKey)

	// Test Deserialization
	returnedRtype, returnedStartLabel, returnedEndLabel, err := deserializeRelBatchKey(batchKey)
	assert.NoError(t, err)
	assert.Equal(t, rtype, returnedRtype)
	assert.Equal(t, startLabel, returnedStartLabel)
	assert.Equal(t, endLabel, returnedEndLabel)
}

func TestBatchSubgraphAddNode(t *testing.T) {
	matchKeys := NewSimpleMatchKeys()
	subgraph := NewBatchSubgraph(matchKeys)
	node := NewEventNode("abc123")

	err := subgraph.AddNode(node)

	assert.NoError(t, err)
	assert.Equal(t, 1, subgraph.NodeCount())
	assert.Equal(t, []*Node{node}, subgraph.nodes["Event:Event"])
}

func TestBatchSubgraphAddNodeInvalid(t *testing.T) {
	matchKeys := NewSimpleMatchKeys()
	subgraph := NewBatchSubgraph(matchKeys)
	node := NewNode("Event", Properties{})

	err := subgraph.AddNode(node)

	assert.ErrorContains(t, err, "invalid node: missing property id")
	assert.Equal(t, 0, subgraph.NodeCount())
}

func TestBatchSubgraphAddRel(t *testing.T) {
	matchKeys := NewSimpleMatchKeys()
	subgraph := NewBatchSubgraph(matchKeys)

	userNode := NewUserNode("pubkey1")
	eventNode := NewEventNode("abc123")
	rel := NewSignedRel(userNode, eventNode, nil)

	err := subgraph.AddRel(rel)

	assert.NoError(t, err)
	assert.Equal(t, 1, subgraph.RelCount())
	assert.Equal(t, []*Relationship{rel}, subgraph.rels["SIGNED,User,Event"])
}

func TestNodeBatches(t *testing.T) {
	matchKeys := NewSimpleMatchKeys()
	subgraph := NewBatchSubgraph(matchKeys)
	node := NewEventNode("abc123")
	subgraph.AddNode(node)

	batches, err := subgraph.NodeBatches()

	assert.NoError(t, err)
	assert.Len(t, batches, 1)
	assert.Equal(t, "Event", batches[0].MatchLabel)
	assert.ElementsMatch(t, []string{"Event"}, batches[0].Labels)
	assert.ElementsMatch(t, []string{"id"}, batches[0].MatchKeys)
	assert.Equal(t, []*Node{node}, batches[0].Nodes)
}

func TestRelBatches(t *testing.T) {
	matchKeys := NewSimpleMatchKeys()
	subgraph := NewBatchSubgraph(matchKeys)
	userNode := NewUserNode("pubkey1")
	eventNode := NewEventNode("abc123")
	rel := NewSignedRel(userNode, eventNode, nil)
	subgraph.AddRel(rel)

	batches, err := subgraph.RelBatches()

	assert.NoError(t, err)
	assert.Len(t, batches, 1)
	assert.Equal(t, "SIGNED", batches[0].Type)
	assert.Equal(t, "User", batches[0].StartLabel)
	assert.ElementsMatch(t, []string{"pubkey"}, batches[0].StartMatchKeys)
	assert.Equal(t, "Event", batches[0].EndLabel)
	assert.ElementsMatch(t, []string{"id"}, batches[0].EndMatchKeys)
	assert.Equal(t, []*Relationship{rel}, batches[0].Rels)
}
