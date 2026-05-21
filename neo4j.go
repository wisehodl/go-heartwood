package heartwood

import (
	"context"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
)

func ConnectNeo4j(ctx context.Context, uri, user, password string) (neo4j.Driver, error) {
	driver, err := neo4j.NewDriver(
		uri,
		neo4j.BasicAuth(user, password, ""))
	if err != nil {
		return nil, err
	}

	err = driver.VerifyConnectivity(ctx)
	if err != nil {
		return nil, err
	}

	return driver, nil
}

// SetNeo4jSchema ensures that the necessary indexes and constraints exist in
// the database
func SetNeo4jSchema(ctx context.Context, driver neo4j.Driver) error {
	schemaQueries := []string{
		// Constraints

		// User.pubkey
		`CREATE CONSTRAINT user IF NOT EXISTS
		 FOR (n:User) REQUIRE n.pubkey IS UNIQUE`,

		// Relay.url
		`CREATE CONSTRAINT url IF NOT EXISTS
		 FOR (n:Relay) REQUIRE n.url IS UNIQUE`,

		// Event.id
		`CREATE CONSTRAINT event IF NOT EXISTS
		 FOR (n:Event) REQUIRE n.id IS UNIQUE`,

		// Tag.(name, value)
		`CREATE CONSTRAINT tag IF NOT EXISTS
		 FOR (n:Tag) REQUIRE (n.name, n.value) IS UNIQUE`,

		// ReplacementKey.(pubkey, kind)
		`CREATE CONSTRAINT replacement_key IF NOT EXISTS
		 FOR (n:ReplacementKey) REQUIRE (n.pubkey, n.kind) IS UNIQUE`,

		// Indexes

		// Event.kind
		`CREATE INDEX event_kind IF NOT EXISTS
		 FOR (n:Event) ON (n.kind)`,

		// Event.created_at
		`CREATE INDEX event_created_at IF NOT EXISTS
		 FOR (n:Event) ON (n.created_at)`,
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
