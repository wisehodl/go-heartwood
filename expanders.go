package heartwood

import (
	"git.wisehodl.dev/jay/go-heartwood/graph"
	roots "git.wisehodl.dev/jay/go-roots/events"
)

type Expander func(e roots.Event, s *EventSubgraph)
type ExpanderRegistry []Expander

func NewExpanderRegistry() ExpanderRegistry {
	return []Expander{}
}

func GetDefaultExpanderRegistry() ExpanderRegistry {
	registry := NewExpanderRegistry()

	registry.Add(ExpandTaggedEvents)
	registry.Add(ExpandTaggedUsers)

	return registry
}

func (r *ExpanderRegistry) Add(m Expander) {
	*r = append(*r, m)
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

		referencedEvent := graph.NewEventNode(value)

		s.AddNode(referencedEvent)
		s.AddRel(graph.NewReferencesEventRel(tagNode, referencedEvent, nil))
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

		referencedEvent := graph.NewUserNode(value)

		s.AddNode(referencedEvent)
		s.AddRel(graph.NewReferencesUserRel(tagNode, referencedEvent, nil))
	}
}

// Helpers

func findTagNode(nodes []*graph.Node, name, value string) *graph.Node {
	for _, node := range nodes {
		if node.Props["name"] == name && node.Props["value"] == value {
			return node
		}
	}
	return nil
}
