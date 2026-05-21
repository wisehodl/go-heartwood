package heartwood

import (
	roots "git.wisehodl.dev/jay/go-roots/events"
)

// Types

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
	var nodes []*Node
	for _, node := range s.nodes {
		if node.Labels.Contains(label) {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

func (s *EventSubgraph) FirstNodeByLabel(label string) *Node {
	for _, node := range s.nodes {
		if node.Labels.Contains(label) {
			return node
		}
	}
	return nil
}

// Helpers

func IsValidTag(t roots.Tag) bool {
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

// Event to subgraph pipeline

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

// Core pipeline functions

func newEventNode(eventID string, createdAt int64, kind int, content string) *Node {
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
	nodes := make([]*Node, 0, len(tags))
	for _, tag := range tags {
		if !IsValidTag(tag) {
			continue
		}
		nodes = append(nodes, NewTagNode(tag[0], tag[1]))
	}
	return nodes
}

func newTagRels(event *Node, tags []*Node) []*Relationship {
	rels := make([]*Relationship, 0, len(tags))
	for _, tag := range tags {
		rels = append(rels, NewTaggedRel(event, tag, nil))
	}
	return rels
}

// Expander Pipeline

type Expander func(e roots.Event, s *EventSubgraph)
type ExpanderPipeline []Expander

func NewExpanderPipeline(expanders ...Expander) ExpanderPipeline {
	return ExpanderPipeline(expanders)
}

func DefaultExpanders() []Expander {
	return []Expander{
		ExpandTaggedEvents,
		ExpandTaggedUsers,
		ExpandReplaceableEvents,
	}
}

func ExpandTaggedEvents(e roots.Event, s *EventSubgraph) {
	for _, tag := range s.NodesByLabel("Tag") {
		if tag.Props["name"] != "e" {
			continue
		}

		id, ok := tag.Props["value"].(string)
		if !ok || !roots.IsValidID(id) {
			continue
		}

		event := NewEventNode(id)

		s.AddNode(event)
		s.AddRel(NewReferencesEventRel(tag, event, nil))
	}
}

func ExpandTaggedUsers(e roots.Event, s *EventSubgraph) {
	for _, tag := range s.NodesByLabel("Tag") {
		if tag.Props["name"] != "p" {
			continue
		}

		pubkey, ok := tag.Props["value"].(string)
		if !ok || !roots.IsValidKey(pubkey) {
			continue
		}

		user := NewUserNode(pubkey)

		s.AddNode(user)
		s.AddRel(NewReferencesUserRel(tag, user, nil))
	}
}

func ExpandReplaceableEvents(e roots.Event, s *EventSubgraph) {
	if e.Kind != 0 && e.Kind != 3 && !(e.Kind >= 10000 && e.Kind < 20000) {
		return
	}

	eventNode := s.FirstNodeByLabel("Event")
	if eventNode == nil {
		return
	}

	var authorNode *Node
	for _, n := range s.NodesByLabel("User") {
		if n.Props["pubkey"] == e.PubKey {
			authorNode = n
			break
		}
	}
	if authorNode == nil {
		return
	}

	rk := NewReplacementKeyNode(e.PubKey, e.Kind)
	s.AddNode(rk)
	s.AddRel(NewIsReplaceableRel(eventNode, rk, nil))
	s.AddRel(NewForUserRel(rk, authorNode, nil))
}
