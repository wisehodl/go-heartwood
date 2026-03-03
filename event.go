package heartwood

import (
	roots "git.wisehodl.dev/jay/go-roots/events"
)

func EventToSubgraph(e roots.Event, exp ExpanderRegistry) *Subgraph {
	subgraph := NewSubgraph()

	// Create Event node
	eventNode := NewEventNode(e.ID)
	eventNode.Props["created_at"] = e.CreatedAt
	eventNode.Props["kind"] = e.Kind
	eventNode.Props["content"] = e.Content

	// Create User node
	userNode := NewUserNode(e.PubKey)

	// Create SIGNED rel
	signedRel := NewSignedRel(userNode, eventNode, nil)

	// Create Tag nodes
	tagNodes := []*Node{}
	for _, tag := range e.Tags {
		if !isValidTag(tag) {
			continue
		}
		tagNodes = append(tagNodes, NewTagNode(tag[0], tag[1]))
	}

	// Create Tag rels
	tagRels := []*Relationship{}
	for _, tagNode := range tagNodes {
		tagRels = append(tagRels, NewTaggedRel(eventNode, tagNode, nil))
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
