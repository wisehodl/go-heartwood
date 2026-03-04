package heartwood

import (
	roots "git.wisehodl.dev/jay/go-roots/events"
)

// Event subgraph struct

type EventSubgraph struct {
	nodes []*Node
	rels  []*Relationship
}

func NewEventSubgraph() *EventSubgraph {
	return &EventSubgraph{
		nodes: []*Node{},
		rels:  []*Relationship{},
	}
}

func (s *EventSubgraph) AddNode(node *Node) {
	s.nodes = append(s.nodes, node)
}

func (s *EventSubgraph) AddRel(rel *Relationship) {
	s.rels = append(s.rels, rel)
}

func (s *EventSubgraph) Nodes() []*Node {
	return s.nodes
}

func (s *EventSubgraph) Rels() []*Relationship {
	return s.rels
}

func (s *EventSubgraph) NodesByLabel(label string) []*Node {
	nodes := []*Node{}
	for _, node := range s.nodes {
		if node.Labels.Contains(label) {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

// Helpers

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

// Event to subgraph conversion

func EventToSubgraph(e roots.Event, p ExpanderPipeline) *EventSubgraph {
	s := NewEventSubgraph()

	// Create core entities
	eventNode := newEventNode(e.ID, e.CreatedAt, e.Kind, e.Content)
	userNode := newUserNode(e.PubKey)
	signedRel := newSignedRel(userNode, eventNode)
	tagNodes := newTagNodes(e.Tags)
	tagRels := newTagRels(eventNode, tagNodes)

	// Populate subgraph
	s.AddNode(eventNode)
	s.AddNode(userNode)
	s.AddRel(signedRel)
	for _, node := range tagNodes {
		s.AddNode(node)
	}
	for _, rel := range tagRels {
		s.AddRel(rel)
	}

	// Run expanders
	for _, expander := range p {
		expander(e, s)
	}

	return s
}

func newEventNode(eventID string, createdAt int, kind int, content string) *Node {
	eventNode := NewEventNode(eventID)
	eventNode.Props["created_at"] = createdAt
	eventNode.Props["kind"] = kind
	eventNode.Props["content"] = content
	return eventNode
}

func newUserNode(pubkey string) *Node {
	return NewUserNode(pubkey)
}

func newSignedRel(user, event *Node) *Relationship {
	return NewSignedRel(user, event, nil)
}

func newTagNodes(tags []roots.Tag) []*Node {
	nodes := []*Node{}
	for _, tag := range tags {
		if !isValidTag(tag) {
			continue
		}
		nodes = append(nodes, NewTagNode(tag[0], tag[1]))
	}
	return nodes
}

func newTagRels(event *Node, tags []*Node) []*Relationship {
	rels := []*Relationship{}
	for _, tag := range tags {
		rels = append(rels, NewTaggedRel(event, tag, nil))
	}
	return rels
}
