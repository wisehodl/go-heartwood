package heartwood

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewRelationshipWithValidation(t *testing.T) {
	cases := []struct {
		name        string
		start       *Node
		end         *Node
		wantErr     bool
		wantErrText string
	}{
		{
			name:  "valid start and end nodes",
			start: NewUserNode("pubkey1"),
			end:   NewEventNode("abc123"),
		},
		{
			name:        "mismatched start node label",
			start:       NewEventNode("abc123"),
			end:         NewEventNode("abc123"),
			wantErr:     true,
			wantErrText: "expected start node to have label 'User'",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rel, err := NewSignedRel(tc.start, tc.end, nil)
			if tc.wantErr {
				assert.Error(t, err)
				assert.ErrorContains(t, err, tc.wantErrText)
				assert.Nil(t, rel)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, "SIGNED", rel.Type)
			assert.Contains(t, rel.Start.Labels.ToArray(), "User")
			assert.Contains(t, rel.End.Labels.ToArray(), "Event")
		})
	}
}
