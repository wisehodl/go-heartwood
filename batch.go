package heartwood

import (
	"context"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
)

func MergeSubgraph(
	ctx context.Context,
	driver neo4j.Driver,
	subgraph *StructuredSubgraph,
) ([]neo4j.ResultSummary, error) {
	// Validate subgraph
	for _, nodeKey := range subgraph.NodeKeys() {
		matchLabel, _, err := DeserializeNodeKey(nodeKey)
		if err != nil {
			return nil, err
		}

		_, exists := subgraph.matchProvider.GetKeys(matchLabel)
		if !exists {
			return nil, fmt.Errorf("unknown match label: %s", matchLabel)
		}
	}

	for _, relKey := range subgraph.RelKeys() {
		_, startLabel, endLabel, err := DeserializeRelKey(relKey)
		if err != nil {
			return nil, err
		}

		_, exists := subgraph.matchProvider.GetKeys(startLabel)
		if !exists {
			return nil, fmt.Errorf("unknown match label: %s", startLabel)
		}

		_, exists = subgraph.matchProvider.GetKeys(endLabel)
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
			matchLabel, labels, _ := DeserializeNodeKey(nodeKey)
			nodeResultSummary, err := MergeNodes(
				ctx, tx,
				matchLabel,
				labels,
				subgraph.matchProvider,
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
			rtype, startLabel, endLabel, _ := DeserializeRelKey(relKey)
			relResultSummary, err := MergeRels(
				ctx, tx,
				rtype,
				startLabel,
				endLabel,
				subgraph.matchProvider,
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
	matchProvider MatchKeysProvider,
	nodes []*Node,
) (*neo4j.ResultSummary, error) {
	cypherLabels := ToCypherLabels(nodeLabels)

	matchKeys, _ := matchProvider.GetKeys(matchLabel)
	cypherProps := ToCypherProps(matchKeys, "node.")

	serializedNodes := []*SerializedNode{}
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
	matchProvider MatchKeysProvider,
	rels []*Relationship,
) (*neo4j.ResultSummary, error) {
	cypherType := ToCypherLabel(rtype)
	startCypherLabel := ToCypherLabel(startLabel)
	endCypherLabel := ToCypherLabel(endLabel)

	matchKeys, _ := matchProvider.GetKeys(startLabel)
	startCypherProps := ToCypherProps(matchKeys, "rel.start.")

	matchKeys, _ = matchProvider.GetKeys(endLabel)
	endCypherProps := ToCypherProps(matchKeys, "rel.end.")

	serializedRels := []*SerializedRel{}
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
