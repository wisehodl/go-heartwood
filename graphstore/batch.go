package graphstore

import (
	"context"
	"fmt"
	"git.wisehodl.dev/jay/go-heartwood/cypher"
	"git.wisehodl.dev/jay/go-heartwood/graph"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
)

func MergeSubgraph(
	ctx context.Context,
	driver neo4j.Driver,
	subgraph *graph.StructuredSubgraph,
) ([]neo4j.ResultSummary, error) {
	// Validate subgraph
	for _, nodeKey := range subgraph.NodeKeys() {
		matchLabel, _, err := graph.DeserializeNodeBatchKey(nodeKey)
		if err != nil {
			return nil, err
		}

		_, exists := subgraph.MatchProvider().GetKeys(matchLabel)
		if !exists {
			return nil, fmt.Errorf("unknown match label: %s", matchLabel)
		}
	}

	for _, relKey := range subgraph.RelKeys() {
		_, startLabel, endLabel, err := graph.DeserializeRelBatchKey(relKey)
		if err != nil {
			return nil, err
		}

		_, exists := subgraph.MatchProvider().GetKeys(startLabel)
		if !exists {
			return nil, fmt.Errorf("unknown match label: %s", startLabel)
		}

		_, exists = subgraph.MatchProvider().GetKeys(endLabel)
		if !exists {
			return nil, fmt.Errorf("unknown match label: %s", endLabel)
		}
	}

	// Merge subgraph
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	defer session.Close(ctx)

	resultSummariesAny, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		var resultSummaries []neo4j.ResultSummary

		for _, nodeKey := range subgraph.NodeKeys() {
			matchLabel, labels, _ := graph.DeserializeNodeBatchKey(nodeKey)
			nodeResultSummary, err := MergeNodes(
				ctx, tx,
				matchLabel,
				labels,
				subgraph.MatchProvider(),
				subgraph.GetNodes(nodeKey),
			)
			if err != nil {
				return nil, fmt.Errorf("failed to merge nodes for key %s: %w", nodeKey, err)
			}
			if nodeResultSummary != nil {
				resultSummaries = append(resultSummaries, *nodeResultSummary)
			}
		}

		for _, relKey := range subgraph.RelKeys() {
			rtype, startLabel, endLabel, _ := graph.DeserializeRelBatchKey(relKey)
			relResultSummary, err := MergeRels(
				ctx, tx,
				rtype,
				startLabel,
				endLabel,
				subgraph.MatchProvider(),
				subgraph.GetRels(relKey),
			)
			if err != nil {
				return nil, fmt.Errorf("failed to merge relationships for key %s: %w", relKey, err)
			}
			if relResultSummary != nil {
				resultSummaries = append(resultSummaries, *relResultSummary)
			}
		}

		return resultSummaries, nil
	})

	if err != nil {
		return nil, fmt.Errorf("subgraph merge transaction failed: %w", err)
	}

	resultSummaries, ok := resultSummariesAny.([]neo4j.ResultSummary)
	if !ok {
		return nil, fmt.Errorf("unexpected type returned from ExecuteWrite: got %T", resultSummariesAny)
	}

	return resultSummaries, nil
}

func MergeNodes(
	ctx context.Context,
	tx neo4j.ManagedTransaction,
	matchLabel string,
	nodeLabels []string,
	matchProvider graph.MatchKeysProvider,
	nodes []*graph.Node,
) (*neo4j.ResultSummary, error) {
	cypherLabels := cypher.ToCypherLabels(nodeLabels)

	matchKeys, _ := matchProvider.GetKeys(matchLabel)
	cypherProps := cypher.ToCypherProps(matchKeys, "node.")

	serializedNodes := []*graph.SerializedNode{}
	for _, node := range nodes {
		serializedNodes = append(serializedNodes, node.Serialize())
	}

	query := fmt.Sprintf(`
		UNWIND $nodes as node

		MERGE (n%s { %s })
		SET n += node
		`,
		cypherLabels, cypherProps,
	)

	result, err := tx.Run(ctx,
		query,
		map[string]any{
			"nodes": serializedNodes,
		})
	if err != nil {
		return nil, err
	}

	summary, err := result.Consume(ctx)
	if err != nil {
		return nil, err
	}

	return &summary, nil
}

func MergeRels(
	ctx context.Context,
	tx neo4j.ManagedTransaction,
	rtype string,
	startLabel string,
	endLabel string,
	matchProvider graph.MatchKeysProvider,
	rels []*graph.Relationship,
) (*neo4j.ResultSummary, error) {
	cypherType := cypher.ToCypherLabel(rtype)
	startCypherLabel := cypher.ToCypherLabel(startLabel)
	endCypherLabel := cypher.ToCypherLabel(endLabel)

	matchKeys, _ := matchProvider.GetKeys(startLabel)
	startCypherProps := cypher.ToCypherProps(matchKeys, "rel.start.")

	matchKeys, _ = matchProvider.GetKeys(endLabel)
	endCypherProps := cypher.ToCypherProps(matchKeys, "rel.end.")

	serializedRels := []*graph.SerializedRel{}
	for _, rel := range rels {
		serializedRels = append(serializedRels, rel.Serialize())
	}

	query := fmt.Sprintf(`
		UNWIND $rels as rel

		MATCH (start%s { %s })
		MATCH (end%s { %s })

		MERGE (start)-[r%s]->(end)
		SET r += rel.props
		`,
		startCypherLabel, startCypherProps,
		endCypherLabel, endCypherProps,
		cypherType,
	)

	result, err := tx.Run(ctx,
		query,
		map[string]any{
			"rels": serializedRels,
		})
	if err != nil {
		return nil, err
	}

	summary, err := result.Consume(ctx)
	if err != nil {
		return nil, err
	}

	return &summary, nil
}
