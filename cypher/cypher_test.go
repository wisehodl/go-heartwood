package cypher

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestToCypherLabel(t *testing.T) {
	assert.Equal(t, ":`Event`", ToCypherLabel("Event"))
}

func TestToCypherLabels(t *testing.T) {
	assert.Equal(t, ":`Event`:`ReplaceableEvent`",
		ToCypherLabels([]string{"Event", "ReplaceableEvent"}))
}

func TestToCypherProps(t *testing.T) {
	cases := []struct {
		name     string
		keys     []string
		prefix   string
		expected string
	}{
		{
			name:     "default prefix",
			keys:     []string{"id", "name"},
			prefix:   "",
			expected: "id: $id, name: $name",
		},
		{
			name:     "set prefix",
			keys:     []string{"id", "name"},
			prefix:   "entity.",
			expected: "id: entity.id, name: entity.name",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, ToCypherProps(tc.keys, tc.prefix))
		})
	}
}
