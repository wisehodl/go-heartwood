package graphstore

import (
	"context"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
)

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
