package heartwood

import (
	"git.wisehodl.dev/jay/go-heartwood/graph"
	roots "git.wisehodl.dev/jay/go-roots/events"
)

// Event subgraph struct

type EventSubgraph struct {
	nodes []*graph.Node
	rels  []*graph.Relationship
}

func NewEventSubgraph() *EventSubgraph {
	return &EventSubgraph{
		nodes: []*graph.Node{},
		rels:  []*graph.Relationship{},
	}
}

func (s *EventSubgraph) AddNode(node *graph.Node) {
	s.nodes = append(s.nodes, node)
}

func (s *EventSubgraph) AddRel(rel *graph.Relationship) {
	s.rels = append(s.rels, rel)
}

func (s *EventSubgraph) Nodes() []*graph.Node {
	return s.nodes
}

func (s *EventSubgraph) Rels() []*graph.Relationship {
	return s.rels
}

func (s *EventSubgraph) NodesByLabel(label string) []*graph.Node {
	nodes := []*graph.Node{}
	for _, node := range s.nodes {
		if node.Labels.Contains(label) {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

// Event to subgraph conversion

func EventToSubgraph(e roots.Event, exp ExpanderRegistry) *EventSubgraph {
	subgraph := NewEventSubgraph()

	// Create Event node
	eventNode := graph.NewEventNode(e.ID)
	eventNode.Props["created_at"] = e.CreatedAt
	eventNode.Props["kind"] = e.Kind
	eventNode.Props["content"] = e.Content

	// Create User node
	userNode := graph.NewUserNode(e.PubKey)

	// Create SIGNED rel
	signedRel := graph.NewSignedRel(userNode, eventNode, nil)

	// Create Tag nodes
	tagNodes := []*graph.Node{}
	for _, tag := range e.Tags {
		if !isValidTag(tag) {
			continue
		}
		tagNodes = append(tagNodes, graph.NewTagNode(tag[0], tag[1]))
	}

	// Create Tag rels
	tagRels := []*graph.Relationship{}
	for _, tagNode := range tagNodes {
		tagRels = append(tagRels, graph.NewTaggedRel(eventNode, tagNode, nil))
	}

	// Populate subgraph
	subgraph.AddNode(eventNode)
	subgraph.AddNode(userNode)
	subgraph.AddRel(signedRel)
	for _, node := range tagNodes {
		subgraph.AddNode(node)
	}
	for _, rel := range tagRels {
		subgraph.AddRel(rel)
	}

	// Run expanders
	for _, expander := range exp {
		expander(e, subgraph)
	}

	return subgraph
}

func isValidTag(t roots.Tag) bool {
	if len(t) < 2 {
		// Skip tags that do not have name and value fields
		return false
	}
	if len(t[0])+len(t[1]) > 8192 {
		// Skip tags that are too large for the neo4j indexer
		return false
	}
	return true
}
