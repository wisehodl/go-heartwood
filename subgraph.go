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
	nodes := make([]*Node, 0, len(tags))
	for _, tag := range tags {
		if !isValidTag(tag) {
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
	}
}

func ExpandTaggedEvents(e roots.Event, s *EventSubgraph) {
	tagNodes := s.NodesByLabel("Tag")
	for _, tag := range e.Tags {
		if !isValidTag(tag) {
			continue
		}
		name := tag[0]
		value := tag[1]

		if name != "e" || !roots.Hex64Pattern.MatchString(value) {
			continue
		}

		tagNode := findTagNode(tagNodes, name, value)
		if tagNode == nil {
			continue
		}

		referencedEvent := NewEventNode(value)

		s.AddNode(referencedEvent)
		s.AddRel(NewReferencesEventRel(tagNode, referencedEvent, nil))
	}
}

func ExpandTaggedUsers(e roots.Event, s *EventSubgraph) {
	tagNodes := s.NodesByLabel("Tag")
	for _, tag := range e.Tags {
		if !isValidTag(tag) {
			continue
		}
		name := tag[0]
		value := tag[1]

		if name != "p" || !roots.Hex64Pattern.MatchString(value) {
			continue
		}

		tagNode := findTagNode(tagNodes, name, value)
		if tagNode == nil {
			continue
		}

		referencedEvent := NewUserNode(value)

		s.AddNode(referencedEvent)
		s.AddRel(NewReferencesUserRel(tagNode, referencedEvent, nil))
	}
}

func findTagNode(nodes []*Node, name, value string) *Node {
	for _, node := range nodes {
		if node.Props["name"] == name && node.Props["value"] == value {
			return node
		}
	}
	return nil
}
