// This module provides methods for creating nodes and relationships according
// to a defined schema.

package heartwood

import (
	"context"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
)

// ========================================
// Schema Match Keys
// ========================================

func NewMatchKeys() *MatchKeys {
	return &MatchKeys{
		keys: map[string][]string{
			"User":  {"pubkey"},
			"Relay": {"url"},
			"Event": {"id"},
			"Tag":   {"name", "value"},
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

func NewTagNode(name string, value string, rest []string) *Node {
	return NewNode("Tag", Properties{
		"name":  name,
		"value": value,
		"rest":  rest})
}

// ========================================
// Relationship Constructors
// ========================================

func NewSignedRel(
	start *Node, end *Node, props Properties) (*Relationship, error) {
	return NewRelationshipWithValidation(
		"SIGNED", "User", "Event", start, end, props)

}

func NewTaggedRel(
	start *Node, end *Node, props Properties) (*Relationship, error) {
	return NewRelationshipWithValidation(
		"TAGGED", "Event", "Tag", start, end, props)
}

func NewReferencesEventRel(
	start *Node, end *Node, props Properties) (*Relationship, error) {
	return NewRelationshipWithValidation(
		"REFERENCES", "Event", "Event", start, end, props)
}

func NewReferencesUserRel(
	start *Node, end *Node, props Properties) (*Relationship, error) {
	return NewRelationshipWithValidation(
		"REFERENCES", "Event", "User", start, end, props)
}

// ========================================
// Relationship Constructor Helpers
// ========================================

func validateNodeLabel(node *Node, role string, expectedLabel string) error {
	if !node.Labels.Contains(expectedLabel) {
		return fmt.Errorf(
			"expected %s node to have label '%s'. got %v",
			role, expectedLabel, node.Labels.ToArray(),
		)
	}

	return nil
}

func NewRelationshipWithValidation(
	rtype string,
	startLabel string,
	endLabel string,
	start *Node,
	end *Node,
	props Properties) (*Relationship, error) {
	var err error

	err = validateNodeLabel(start, "start", startLabel)
	if err != nil {
		return nil, err
	}

	err = validateNodeLabel(end, "end", endLabel)
	if err != nil {
		return nil, err
	}

	return NewRelationship(rtype, start, end, props), nil
}

// ========================================
// Schema Indexes and Constaints
// ========================================

// SetNeo4jSchema ensures that the necessary indexes and constraints exist in
// the database
func SetNeo4jSchema(ctx context.Context, driver neo4j.Driver) error {
	schemaQueries := []string{
		`CREATE CONSTRAINT user_pubkey IF NOT EXISTS
		 FOR (n:User) REQUIRE n.pubkey IS UNIQUE`,

		`CREATE INDEX user_pubkey IF NOT EXISTS
		 FOR (n:User) ON (n.pubkey)`,

		`CREATE INDEX event_id IF NOT EXISTS
		 FOR (n:Event) ON (n.id)`,

		`CREATE INDEX event_kind IF NOT EXISTS
		 FOR (n:Event) ON (n.kind)`,

		`CREATE INDEX tag_name_value IF NOT EXISTS
		 FOR (n:Tag) ON (n.name, n.value)`,
	}

	// Create indexes and constraints
	for _, query := range schemaQueries {
		_, err := neo4j.ExecuteQuery(ctx, driver,
			query,
			nil,
			neo4j.EagerResultTransformer,
			neo4j.ExecuteQueryWithDatabase("neo4j"))

		if err != nil {
			return err
		}
	}

	return nil
}
