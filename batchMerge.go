package heartwood

import (
	"context"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
	"strings"
)

// Structs

type NodeBatch struct {
	MatchLabel string
	Labels     []string
	MatchKeys  []string
	Nodes      []*Node
}

type RelBatch struct {
	Type           string
	StartLabel     string
	StartMatchKeys []string
	EndLabel       string
	EndMatchKeys   []string
	Rels           []*Relationship
}

type BatchSubgraph struct {
	nodes         map[string][]*Node
	rels          map[string][]*Relationship
	matchProvider MatchKeysProvider
}

func NewBatchSubgraph(matchProvider MatchKeysProvider) *BatchSubgraph {
	return &BatchSubgraph{
		nodes:         make(map[string][]*Node),
		rels:          make(map[string][]*Relationship),
		matchProvider: matchProvider,
	}
}

func (s *BatchSubgraph) AddNode(node *Node) error {

	// Verify that the node has defined match property values.
	matchLabel, _, err := node.MatchProps(s.matchProvider)
	if err != nil {
		return fmt.Errorf("invalid node: %w", err)
	}

	// Determine the node's batch key.
	batchKey := createNodeBatchKey(matchLabel, node.Labels.AsSortedArray())

	// Add the node to the sub
	s.nodes[batchKey] = append(s.nodes[batchKey], node)

	return nil
}

func (s *BatchSubgraph) AddRel(rel *Relationship) error {

	// Verify that the start node has defined match property values.
	startLabel, _, err := rel.Start.MatchProps(s.matchProvider)
	if err != nil {
		return fmt.Errorf("invalid start node: %w", err)
	}

	// Verify that the end node has defined match property values.
	endLabel, _, err := rel.End.MatchProps(s.matchProvider)
	if err != nil {
		return fmt.Errorf("invalid end node: %w", err)
	}

	// Determine the relationship's batch key.
	batchKey := createRelBatchKey(rel.Type, startLabel, endLabel)

	// Add the relationship to the sub
	s.rels[batchKey] = append(s.rels[batchKey], rel)

	return nil
}

func (s *BatchSubgraph) NodeCount() int {
	count := 0
	for l := range s.nodes {
		count += len(s.nodes[l])
	}
	return count
}

func (s *BatchSubgraph) RelCount() int {
	count := 0
	for t := range s.rels {
		count += len(s.rels[t])
	}
	return count
}

func (s *BatchSubgraph) nodeKeys() []string {
	keys := make([]string, 0, len(s.nodes))
	for l := range s.nodes {
		keys = append(keys, l)
	}
	return keys
}

func (s *BatchSubgraph) relKeys() []string {
	keys := make([]string, 0, len(s.rels))
	for t := range s.rels {
		keys = append(keys, t)
	}
	return keys
}

func (s *BatchSubgraph) NodeBatches() ([]NodeBatch, error) {
	batches := make([]NodeBatch, 0, len(s.nodeKeys()))

	for _, nodeKey := range s.nodeKeys() {
		matchLabel, labels, err := deserializeNodeBatchKey(nodeKey)
		if err != nil {
			return nil, err
		}
		matchKeys, exists := s.matchProvider.GetKeys(matchLabel)
		if !exists {
			return nil, fmt.Errorf("unknown match label: %s", matchLabel)
		}
		nodes := s.nodes[nodeKey]
		batch := NodeBatch{
			MatchLabel: matchLabel,
			Labels:     labels,
			MatchKeys:  matchKeys,
			Nodes:      nodes,
		}
		batches = append(batches, batch)
	}

	return batches, nil
}

func (s *BatchSubgraph) RelBatches() ([]RelBatch, error) {
	batches := make([]RelBatch, 0, len(s.relKeys()))

	for _, relKey := range s.relKeys() {
		rtype, startLabel, endLabel, err := deserializeRelBatchKey(relKey)
		if err != nil {
			return nil, err
		}

		startMatchKeys, exists := s.matchProvider.GetKeys(startLabel)
		if !exists {
			return nil, fmt.Errorf("unknown match label: %s", startLabel)
		}

		endMatchKeys, exists := s.matchProvider.GetKeys(endLabel)
		if !exists {
			return nil, fmt.Errorf("unknown match label: %s", endLabel)
		}

		rels := s.rels[relKey]
		batch := RelBatch{
			Type:           rtype,
			StartLabel:     startLabel,
			StartMatchKeys: startMatchKeys,
			EndLabel:       endLabel,
			EndMatchKeys:   endMatchKeys,
			Rels:           rels,
		}
		batches = append(batches, batch)
	}

	return batches, nil
}

// Helpers

func createNodeBatchKey(matchLabel string, sortedLabels []string) string {
	serializedLabels := strings.Join(sortedLabels, ",")
	return fmt.Sprintf("%s:%s", matchLabel, serializedLabels)
}

func createRelBatchKey(
	rtype string, startLabel string, endLabel string) string {
	return strings.Join([]string{rtype, startLabel, endLabel}, ",")
}

func deserializeNodeBatchKey(batchKey string) (string, []string, error) {
	parts := strings.Split(batchKey, ":")
	if len(parts) != 2 {
		return "", nil, fmt.Errorf("invalid node batch key: %s", batchKey)
	}
	matchLabel, serializedLabels := parts[0], parts[1]
	labels := strings.Split(serializedLabels, ",")
	return matchLabel, labels, nil
}

func deserializeRelBatchKey(batchKey string) (string, string, string, error) {
	parts := strings.Split(batchKey, ",")
	if len(parts) != 3 {
		return "", "", "", fmt.Errorf("invalid relationship batch key: %s", batchKey)
	}
	rtype, startLabel, endLabel := parts[0], parts[1], parts[2]
	return rtype, startLabel, endLabel, nil
}

// Merge functions

func MergeSubgraph(
	ctx context.Context,
	driver neo4j.Driver,
	subgraph *BatchSubgraph,
) ([]neo4j.ResultSummary, error) {
	nodeBatches, err := subgraph.NodeBatches()
	if err != nil {
		return nil, err
	}
	relBatches, err := subgraph.RelBatches()
	if err != nil {
		return nil, err
	}

	// Merge subgraph
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	defer session.Close(ctx)

	resultSummariesAny, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		var resultSummaries []neo4j.ResultSummary

		for _, nodeBatch := range nodeBatches {
			nodeResultSummary, err := MergeNodes(ctx, tx, nodeBatch)
			if err != nil {
				return nil, fmt.Errorf(
					"failed to merge nodes on label %q (labels %v): %w",
					nodeBatch.MatchLabel,
					nodeBatch.Labels,
					err,
				)
			}
			if nodeResultSummary != nil {
				resultSummaries = append(resultSummaries, nodeResultSummary)
			}
		}

		for _, relBatch := range relBatches {
			relResultSummary, err := MergeRels(ctx, tx, relBatch)
			if err != nil {
				return nil, fmt.Errorf(
					"failed to merge relationships on (%s)-[%s]->(%s): %w",
					relBatch.StartLabel,
					relBatch.Type,
					relBatch.EndLabel,
					err,
				)
			}
			if relResultSummary != nil {
				resultSummaries = append(resultSummaries, relResultSummary)
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
	batch NodeBatch,
) (neo4j.ResultSummary, error) {
	cypherLabels := ToCypherLabels(batch.Labels)
	cypherProps := ToCypherProps(batch.MatchKeys, "node.")

	serializedNodes := make([]*SerializedNode, 0, len(batch.Nodes))
	for _, node := range batch.Nodes {
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

	return summary, nil
}

func MergeRels(
	ctx context.Context,
	tx neo4j.ManagedTransaction,
	batch RelBatch,
) (neo4j.ResultSummary, error) {
	cypherType := ToCypherLabel(batch.Type)
	startCypherLabel := ToCypherLabel(batch.StartLabel)
	endCypherLabel := ToCypherLabel(batch.EndLabel)
	startCypherProps := ToCypherProps(batch.StartMatchKeys, "rel.start.")
	endCypherProps := ToCypherProps(batch.EndMatchKeys, "rel.end.")

	serializedRels := make([]*SerializedRel, 0, len(batch.Rels))
	for _, rel := range batch.Rels {
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

	return summary, nil
}
