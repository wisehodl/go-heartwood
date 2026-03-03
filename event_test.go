package heartwood

import (
	"fmt"
	roots "git.wisehodl.dev/jay/go-roots/events"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

var ids = map[string]string{
	"a": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	"b": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	"c": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
	"d": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
}

var static = roots.Event{
	CreatedAt: 1000,
	Kind:      1,
	Content:   "hello",
}

func newFullEventNode(id string, createdAt, kind int, content string) *Node {
	n := NewEventNode(id)
	n.Props["created_at"] = createdAt
	n.Props["kind"] = kind
	n.Props["content"] = content
	return n
}

func baseSubgraph(eventID, pubkey string) (*Subgraph, *Node, *Node) {
	s := NewSubgraph()
	eventNode := newFullEventNode(eventID, static.CreatedAt, static.Kind, static.Content)
	userNode := NewUserNode(pubkey)
	s.AddNode(eventNode)
	s.AddNode(userNode)
	s.AddRel(NewSignedRel(userNode, eventNode, nil))
	return s, eventNode, userNode
}

func TestEventToSubgraph(t *testing.T) {
	cases := []struct {
		name     string
		event    roots.Event
		expected *Subgraph
	}{
		{
			name: "bare event",
			event: roots.Event{
				ID: ids["a"], PubKey: ids["b"],
				CreatedAt: static.CreatedAt, Kind: static.Kind, Content: static.Content,
			},
			expected: func() *Subgraph {
				s, _, _ := baseSubgraph(ids["a"], ids["b"])
				return s
			}(),
		},
		{
			name: "single generic tag",
			event: roots.Event{
				ID: ids["a"], PubKey: ids["b"],
				CreatedAt: static.CreatedAt, Kind: static.Kind, Content: static.Content,
				Tags: []roots.Tag{{"t", "bitcoin"}},
			},
			expected: func() *Subgraph {
				s, eventNode, _ := baseSubgraph(ids["a"], ids["b"])
				tagNode := NewTagNode("t", "bitcoin")
				s.AddNode(tagNode)
				s.AddRel(NewTaggedRel(eventNode, tagNode, nil))
				return s
			}(),
		},
		{
			name: "tag with fewer than 2 elements",
			event: roots.Event{
				ID: ids["a"], PubKey: ids["b"],
				CreatedAt: static.CreatedAt, Kind: static.Kind, Content: static.Content,
				Tags: []roots.Tag{{"t"}},
			},
			expected: func() *Subgraph {
				s, _, _ := baseSubgraph(ids["a"], ids["b"])
				return s
			}(),
		},
		{
			name: "e tag with valid hex64",
			event: roots.Event{
				ID: ids["a"], PubKey: ids["b"],
				CreatedAt: static.CreatedAt, Kind: static.Kind, Content: static.Content,
				Tags: []roots.Tag{{"e", ids["c"]}},
			},
			expected: func() *Subgraph {
				s, eventNode, _ := baseSubgraph(ids["a"], ids["b"])
				tagNode := NewTagNode("e", ids["c"])
				referencedEvent := NewEventNode(ids["c"])
				s.AddNode(tagNode)
				s.AddNode(referencedEvent)
				s.AddRel(NewTaggedRel(eventNode, tagNode, nil))
				s.AddRel(NewReferencesEventRel(tagNode, referencedEvent, nil))
				return s
			}(),
		},
		{
			name: "e tag with invalid value",
			event: roots.Event{
				ID: ids["a"], PubKey: ids["b"],
				CreatedAt: static.CreatedAt, Kind: static.Kind, Content: static.Content,
				Tags: []roots.Tag{{"e", "notvalid"}},
			},
			expected: func() *Subgraph {
				s, eventNode, _ := baseSubgraph(ids["a"], ids["b"])
				tagNode := NewTagNode("e", "notvalid")
				s.AddNode(tagNode)
				s.AddRel(NewTaggedRel(eventNode, tagNode, nil))
				return s
			}(),
		},
		{
			name: "p tag with valid hex64",
			event: roots.Event{
				ID: ids["a"], PubKey: ids["b"],
				CreatedAt: static.CreatedAt, Kind: static.Kind, Content: static.Content,
				Tags: []roots.Tag{{"p", ids["d"]}},
			},
			expected: func() *Subgraph {
				s, eventNode, _ := baseSubgraph(ids["a"], ids["b"])
				tagNode := NewTagNode("p", ids["d"])
				referencedUser := NewUserNode(ids["d"])
				s.AddNode(tagNode)
				s.AddNode(referencedUser)
				s.AddRel(NewTaggedRel(eventNode, tagNode, nil))
				s.AddRel(NewReferencesUserRel(tagNode, referencedUser, nil))
				return s
			}(),
		},
		{
			name: "p tag with invalid value",
			event: roots.Event{
				ID: ids["a"], PubKey: ids["b"],
				CreatedAt: static.CreatedAt, Kind: static.Kind, Content: static.Content,
				Tags: []roots.Tag{{"p", "notvalid"}},
			},
			expected: func() *Subgraph {
				s, eventNode, _ := baseSubgraph(ids["a"], ids["b"])
				tagNode := NewTagNode("p", "notvalid")
				s.AddNode(tagNode)
				s.AddRel(NewTaggedRel(eventNode, tagNode, nil))
				return s
			}(),
		},
	}

	expanders := GetDefaultExpanderRegistry()

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := EventToSubgraph(tc.event, expanders)
			assertSubgraphsEqual(t, tc.expected, got)
		})
	}
}

// helpers

func nodesEqual(expected, got *Node) error {
	// Compare label counts
	if expected.Labels.Length() != got.Labels.Length() {
		return fmt.Errorf(
			"number of labels does not match. expected %d, got %d",
			expected.Labels.Length(), got.Labels.Length())
	}

	// Compare label values
	for _, label := range expected.Labels.ToArray() {
		if !got.Labels.Contains(label) {
			return fmt.Errorf("missing label %q", label)
		}
	}

	// Compare property values
	if err := propsEqual(expected.Props, got.Props); err != nil {
		return err
	}

	return nil
}

func relsEqual(expected, got *Relationship) error {
	// Compare type
	if expected.Type != got.Type {
		return fmt.Errorf("type: expected %q, got %q", expected.Type, got.Type)
	}

	// Compare property values
	if err := propsEqual(expected.Props, got.Props); err != nil {
		return err
	}

	// Compare endpoints
	if err := nodesEqual(expected.Start, got.Start); err != nil {
		return fmt.Errorf("start node: %w", err)
	}
	if err := nodesEqual(expected.End, got.End); err != nil {
		return fmt.Errorf("end node: %w", err)
	}

	return nil
}

func propsEqual(expected, got Properties) error {
	if len(expected) != len(got) {
		return fmt.Errorf(
			"number of props does not match. expected %d, got %d",
			len(expected), len(got))
	}

	for key, expectedVal := range expected {
		gotVal, exists := got[key]
		if !exists {
			return fmt.Errorf("missing prop %q", key)
		}
		if !reflect.DeepEqual(expectedVal, gotVal) {
			return fmt.Errorf("prop %q: expected %v, got %v", key, expectedVal, gotVal)
		}
	}
	return nil
}

func assertSubgraphsEqual(t *testing.T, expected, got *Subgraph) {
	t.Helper()

	gotNodes := make([]*Node, len(got.Nodes()))
	copy(gotNodes, got.Nodes())

	gotRels := make([]*Relationship, len(got.Rels()))
	copy(gotRels, got.Rels())

	for _, expectedNode := range expected.Nodes() {
		index := findInList(expectedNode, gotNodes, nodesEqual)
		if index == -1 {
			assert.Fail(t, fmt.Sprintf("missing expected node: %+v", expectedNode))
			continue
		}
		gotNodes = removeFromList(index, gotNodes)
	}

	for _, expectedRel := range expected.Rels() {
		index := findInList(expectedRel, gotRels, relsEqual)
		if index == -1 {
			assert.Fail(t, fmt.Sprintf("missing expected rel: %+v", expectedRel))
			continue
		}
		gotRels = removeFromList(index, gotRels)
	}

	assert.Empty(t, gotNodes, "unexpected nodes in subgraph")
	assert.Empty(t, gotRels, "unexpected rels in subgraph")
}

func findInList[T any](item *T, list []*T, equal func(*T, *T) error) int {
	for i, candidate := range list {
		if equal(item, candidate) == nil {
			return i
		}
	}
	return -1
}

func removeFromList[T any](i int, list []*T) []*T {
	return append(list[:i], list[i+1:]...)
}
