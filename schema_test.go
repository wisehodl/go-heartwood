package heartwood

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewRelationshipWithValidation(t *testing.T) {
	cases := []struct {
		name          string
		start         *Node
		end           *Node
		wantPanic     bool
		wantPanicText string
	}{
		{
			name:  "valid start and end nodes",
			start: NewUserNode("pubkey1"),
			end:   NewEventNode("abc123"),
		},
		{
			name:          "mismatched start node label",
			start:         NewEventNode("abc123"),
			end:           NewEventNode("abc123"),
			wantPanic:     true,
			wantPanicText: "expected start node to have label \"User\". got [Event]",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.wantPanic {
				assert.PanicsWithError(t, tc.wantPanicText, func() {
					NewSignedRel(tc.start, tc.end, nil)
				})
				return
			}
			rel := NewSignedRel(tc.start, tc.end, nil)
			assert.Equal(t, "SIGNED", rel.Type)
			assert.Contains(t, rel.Start.Labels.AsSortedArray(), "User")
			assert.Contains(t, rel.End.Labels.AsSortedArray(), "Event")
		})
	}
}
