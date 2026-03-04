package heartwood

import (
	"context"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
)

// Interface

type GraphDB interface {
	MergeSubgraph(ctx context.Context, subgraph *BatchSubgraph) ([]neo4j.ResultSummary, error)
}

func NewGraphDriver(driver neo4j.Driver) GraphDB {
	return &graphdb{driver: driver}
}

type graphdb struct {
	driver neo4j.Driver
}

func (n *graphdb) MergeSubgraph(ctx context.Context, subgraph *BatchSubgraph) ([]neo4j.ResultSummary, error) {
	return MergeSubgraph(ctx, n.driver, subgraph)
}

// Functions

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
