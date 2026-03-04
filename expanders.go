package heartwood

import (
	roots "git.wisehodl.dev/jay/go-roots/events"
)

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

// Default Expander Functions

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

// Helpers

func findTagNode(nodes []*Node, name, value string) *Node {
	for _, node := range nodes {
		if node.Props["name"] == name && node.Props["value"] == value {
			return node
		}
	}
	return nil
}
