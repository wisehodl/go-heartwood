package heartwood

import (
	"fmt"
	"strings"
)

// ========================================
// Cypher Formatting Functions
// ========================================

// ToCypherLabel converts a node label or relationship type into its Cypher
// format.
func ToCypherLabel(label string) string {
	return fmt.Sprintf(":`%s`", label)
}

// ToCypherLabels converts a list of node labels into its Cypher format.
func ToCypherLabels(labels []string) string {
	var cypherLabels []string

	for _, label := range labels {
		cypherLabels = append(cypherLabels, ToCypherLabel(label))
	}

	return strings.Join(cypherLabels, "")
}

func ToCypherProps(keys []string, prefix string) string {
	if prefix == "" {
		prefix = "$"
	}

	var cypherPropsParts []string
	for _, key := range keys {
		cypherPropsParts = append(
			cypherPropsParts, fmt.Sprintf("%s: %s%s", key, prefix, key))
	}
	return strings.Join(cypherPropsParts, ", ")
}
