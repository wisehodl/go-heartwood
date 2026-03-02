package heartwood

import (
	"context"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
)

// ConnectNeo4j creates a new Neo4j driver and verifies its connectivity.
func ConnectNeo4j(ctx context.Context, uri, user, password string) (neo4j.Driver, error) {
	driver, err := neo4j.NewDriver(
		uri,
		neo4j.BasicAuth(user, password, ""))

	err = driver.VerifyConnectivity(ctx)
	if err != nil {
		return driver, err
	}

	return driver, nil
}
