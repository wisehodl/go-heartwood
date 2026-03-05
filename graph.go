package heartwood

import (
	"fmt"
)

// ========================================
// Types
// ========================================

// Properties represents a map of node or relationship props.
type Properties map[string]any

// ========================================
// Match Key Provider
// ========================================

// MatchKeysProvider defines methods for querying a mapping of node labels and
// the property keys used to match nodes with them.
type MatchKeysProvider interface {
	// GetLabels returns the array of node labels in the mapping.
	GetLabels() []string

	// GetKeys returns the node property keys used to match nodes with the
	// given label and a boolean indicating the success of the lookup.
	GetKeys(label string) ([]string, bool)
}

// SimpleMatchKeys is a simple implementation of the MatchKeysProvider interface.
type SimpleMatchKeys struct {
	Keys map[string][]string
}

func (p *SimpleMatchKeys) GetLabels() []string {
	labels := make([]string, 0, len(p.Keys))
	for l := range p.Keys {
		labels = append(labels, l)
	}
	return labels
}

func (p *SimpleMatchKeys) GetKeys(label string) ([]string, bool) {
	if keys, exists := p.Keys[label]; exists {
		return keys, exists
	}
	return nil, false
}

// ========================================
// Nodes
// ========================================

// Node represents a Neo4j node entity, encapsulating its labels and
// properties.
type Node struct {
	// Set of labels on the node.
	Labels *StringSet
	// Mapping of properties on the node.
	Props Properties
}

// NewNode creates a new node with the given label and properties.
func NewNode(label string, props Properties) *Node {
	if props == nil {
		props = make(Properties)
	}
	return &Node{
		Labels: NewStringSet(label),
		Props:  props,
	}
}

// MatchProps returns the node label and the property values to match it in the
// database.
func (n *Node) MatchProps(
	matchProvider MatchKeysProvider) (string, Properties, error) {

	// Iterate over each label on the node, checking whether each has match
	// keys associated with it.
	labels := n.Labels.AsSortedArray()
	for _, label := range labels {
		if keys, exists := matchProvider.GetKeys(label); exists {
			props := make(Properties)

			// Get the property values associated with each match key.
			for _, key := range keys {
				if value, exists := n.Props[key]; exists {
					props[key] = value
				} else {

					// If any match property values are missing, return an
					// error.
					return label, nil,
						fmt.Errorf(
							"missing property %s for label %s", key, label)
				}
			}

			// Return the label and match properties
			return label, props, nil
		}
	}

	// If none of the node labels have defined match keys, return an error.
	return "", nil, fmt.Errorf("no recognized label found in %v", n.Labels)
}

type SerializedNode = Properties

func (n *Node) Serialize() *SerializedNode {
	return &n.Props
}

// ========================================
// Relationships
// ========================================

// Relationship represents a Neo4j relationship between two nodes, including
// its type and properties.
type Relationship struct {
	// The relationship type.
	Type string
	// The start node for the relationship.
	Start *Node
	// The end node for the relationship.
	End *Node
	// Mapping of properties on the relationship
	Props Properties
}

// NewRelationship creates a new relationship with the given type, start node,
// end node, and properties
func NewRelationship(
	rtype string, start *Node, end *Node, props Properties) *Relationship {

	if props == nil {
		props = make(Properties)
	}
	return &Relationship{
		Type:  rtype,
		Start: start,
		End:   end,
		Props: props,
	}
}

type SerializedRel = map[string]Properties

func (r *Relationship) Serialize() *SerializedRel {
	srel := make(map[string]Properties)
	srel["props"] = r.Props
	srel["start"] = r.Start.Props
	srel["end"] = r.End.Props
	return &srel
}
