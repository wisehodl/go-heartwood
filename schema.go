package heartwood

import (
	"fmt"
)

// ========================================
// Schema Match Keys
// ========================================

func NewSimpleMatchKeys() *SimpleMatchKeys {
	return &SimpleMatchKeys{
		Keys: map[string][]string{
			"User":           {"pubkey"},
			"Relay":          {"url"},
			"Event":          {"id"},
			"Tag":            {"name", "value"},
			"ReplacementKey": {"pubkey", "kind"},
		},
	}
}

// ========================================
// Node Constructors
// ========================================

func NewUserNode(pubkey string) *Node {
	return NewNode("User", Properties{"pubkey": pubkey})
}

func NewRelayNode(url string) *Node {
	return NewNode("Relay", Properties{"url": url})
}

func NewEventNode(id string) *Node {
	return NewNode("Event", Properties{"id": id})
}

func NewTagNode(name string, value string) *Node {
	return NewNode("Tag", Properties{
		"name":  name,
		"value": value})
}

func NewReplacementKeyNode(pubkey string, kind int) *Node {
	return NewNode("ReplacementKey", Properties{
		"pubkey": pubkey,
		"kind":   kind,
	})
}

// ========================================
// Relationship Constructors
// ========================================

func NewSignedRel(
	start *Node, end *Node, props Properties) *Relationship {
	return NewRelationshipWithValidation(
		"SIGNED", "User", "Event", start, end, props)

}

func NewTaggedRel(
	start *Node, end *Node, props Properties) *Relationship {
	return NewRelationshipWithValidation(
		"TAGGED", "Event", "Tag", start, end, props)
}

func NewReferencesEventRel(
	start *Node, end *Node, props Properties) *Relationship {
	return NewRelationshipWithValidation(
		"REFERENCES", "Tag", "Event", start, end, props)
}

func NewReferencesUserRel(
	start *Node, end *Node, props Properties) *Relationship {
	return NewRelationshipWithValidation(
		"REFERENCES", "Tag", "User", start, end, props)
}

func NewIsReplaceableRel(
	start *Node, end *Node, props Properties) *Relationship {
	return NewRelationshipWithValidation(
		"IS_REPLACEABLE", "Event", "ReplacementKey", start, end, props)
}

func NewForUserRel(
	start *Node, end *Node, props Properties) *Relationship {
	return NewRelationshipWithValidation(
		"FOR_USER", "ReplacementKey", "User", start, end, props)
}

func NewWithDTagRel(
	start *Node, end *Node, props Properties) *Relationship {
	return NewRelationshipWithValidation(
		"WITH_D_TAG", "ReplacementKey", "Tag", start, end, props)
}

func NewReferencesReplacementKeyRel(
	start *Node, end *Node, props Properties) *Relationship {
	return NewRelationshipWithValidation(
		"REFERENCES", "Tag", "ReplacementKey", start, end, props)
}

// ========================================
// Relationship Constructor Helpers
// ========================================

func validateNodeLabel(node *Node, role string, expectedLabel string) {
	if !node.Labels.Contains(expectedLabel) {
		panic(fmt.Errorf(
			"expected %s node to have label %q. got %v",
			role, expectedLabel, node.Labels.AsSortedArray(),
		))
	}
}

func NewRelationshipWithValidation(
	rtype string,
	startLabel string,
	endLabel string,
	start *Node,
	end *Node,
	props Properties) *Relationship {

	validateNodeLabel(start, "start", startLabel)
	validateNodeLabel(end, "end", endLabel)

	return NewRelationship(rtype, start, end, props)
}
