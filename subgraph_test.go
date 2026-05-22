package heartwood

import (
	"fmt"
	roots "git.wisehodl.dev/jay/go-roots/events"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func baseSubgraph(e roots.ValidatedEvent) (*EventSubgraph, *Node, *Node) {
	s := NewEventSubgraph()
	eventNode := newEventNode(e.ID(), e.CreatedAt(), e.Kind(), e.Content())
	userNode := NewUserNode(e.PubKey())
	s.AddNode(eventNode)
	s.AddNode(userNode)
	s.AddRel(NewSignedRel(userNode, eventNode, nil))
	return s, eventNode, userNode
}

func TestEventToSubgraph(t *testing.T) {
	fx := LoadFixtures(t)

	cases := []struct {
		name     string
		event    roots.ValidatedEvent
		expected *EventSubgraph
	}{
		{
			name:  "bare event",
			event: fx.ValidatedEvent(t, "bare"),
			expected: func() *EventSubgraph {
				e := fx.ValidatedEvent(t, "bare")
				s, _, _ := baseSubgraph(e)
				return s
			}(),
		},
		{
			name:  "single generic tag",
			event: fx.ValidatedEvent(t, "generic_tag"),
			expected: func() *EventSubgraph {
				e := fx.ValidatedEvent(t, "generic_tag")
				s, eventNode, _ := baseSubgraph(e)
				tagNode := NewTagNode("t", "bitcoin")
				s.AddNode(tagNode)
				s.AddRel(NewTaggedRel(eventNode, tagNode, nil))
				return s
			}(),
		},
		{
			name:  "e tag with valid hex64",
			event: fx.ValidatedEvent(t, "e_tag_valid"),
			expected: func() *EventSubgraph {
				e := fx.ValidatedEvent(t, "e_tag_valid")
				carolID := fx.ValidatedEvent(t, "carol_placeholder").ID()
				s, eventNode, _ := baseSubgraph(e)
				tagNode := NewTagNode("e", carolID)
				referencedEvent := NewEventNode(carolID)
				s.AddNode(tagNode)
				s.AddNode(referencedEvent)
				s.AddRel(NewTaggedRel(eventNode, tagNode, nil))
				s.AddRel(NewReferencesEventRel(tagNode, referencedEvent, nil))
				return s
			}(),
		},
		{
			name:  "e tag with invalid value",
			event: fx.ValidatedEvent(t, "e_tag_invalid"),
			expected: func() *EventSubgraph {
				e := fx.ValidatedEvent(t, "e_tag_invalid")
				s, eventNode, _ := baseSubgraph(e)
				tagNode := NewTagNode("e", "notvalid")
				s.AddNode(tagNode)
				s.AddRel(NewTaggedRel(eventNode, tagNode, nil))
				return s
			}(),
		},
		{
			name:  "p tag with valid hex64",
			event: fx.ValidatedEvent(t, "p_tag_valid"),
			expected: func() *EventSubgraph {
				e := fx.ValidatedEvent(t, "p_tag_valid")
				bobPubkey := fx.Keys["bob"]
				s, eventNode, _ := baseSubgraph(e)
				tagNode := NewTagNode("p", bobPubkey)
				referencedUser := NewUserNode(bobPubkey)
				s.AddNode(tagNode)
				s.AddNode(referencedUser)
				s.AddRel(NewTaggedRel(eventNode, tagNode, nil))
				s.AddRel(NewReferencesUserRel(tagNode, referencedUser, nil))
				return s
			}(),
		},
		{
			name:  "p tag with invalid value",
			event: fx.ValidatedEvent(t, "p_tag_invalid"),
			expected: func() *EventSubgraph {
				e := fx.ValidatedEvent(t, "p_tag_invalid")
				s, eventNode, _ := baseSubgraph(e)
				tagNode := NewTagNode("p", "notvalid")
				s.AddNode(tagNode)
				s.AddRel(NewTaggedRel(eventNode, tagNode, nil))
				return s
			}(),
		},
		{
			name:  "replaceable kind 0",
			event: fx.ValidatedEvent(t, "replaceable_k0"),
			expected: func() *EventSubgraph {
				e := fx.ValidatedEvent(t, "replaceable_k0")
				s, eventNode, userNode := baseSubgraph(e)
				rk := NewReplacementKeyNode(e.PubKey(), e.Kind())
				s.AddNode(rk)
				s.AddRel(NewIsReplaceableRel(eventNode, rk, nil))
				s.AddRel(NewForUserRel(rk, userNode, nil))
				return s
			}(),
		},
		{
			name:  "replaceable kind 3",
			event: fx.ValidatedEvent(t, "replaceable_k3"),
			expected: func() *EventSubgraph {
				e := fx.ValidatedEvent(t, "replaceable_k3")
				s, eventNode, userNode := baseSubgraph(e)
				rk := NewReplacementKeyNode(e.PubKey(), e.Kind())
				s.AddNode(rk)
				s.AddRel(NewIsReplaceableRel(eventNode, rk, nil))
				s.AddRel(NewForUserRel(rk, userNode, nil))
				return s
			}(),
		},
		{
			name:  "replaceable kind 10000-19999",
			event: fx.ValidatedEvent(t, "replaceable_k10k"),
			expected: func() *EventSubgraph {
				e := fx.ValidatedEvent(t, "replaceable_k10k")
				s, eventNode, userNode := baseSubgraph(e)
				rk := NewReplacementKeyNode(e.PubKey(), e.Kind())
				s.AddNode(rk)
				s.AddRel(NewIsReplaceableRel(eventNode, rk, nil))
				s.AddRel(NewForUserRel(rk, userNode, nil))
				return s
			}(),
		},
	}

	expanders := NewExpanderPipeline(DefaultExpanders()...)

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
	for _, label := range expected.Labels.AsSortedArray() {
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

func assertSubgraphsEqual(t *testing.T, expected, got *EventSubgraph) {
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
