package heartwood

import (
	"encoding/json"
	"os"
	"testing"

	roots "git.wisehodl.dev/jay/go-roots/events"
)

// Fixtures holds the loaded test fixture data from testdata/events.json.
type Fixtures struct {
	Events map[string]FixtureEvent `json:"events"`
	Keys   map[string]string       `json:"keys"`
}

// FixtureEvent is a single entry from the fixtures file.
type FixtureEvent struct {
	Description string     `json:"description"`
	Event       roots.Event `json:"event"`
}

// LoadFixtures reads testdata/events.json and returns the parsed fixtures.
// Calls t.Fatal if the file cannot be read or parsed.
func LoadFixtures(t *testing.T) Fixtures {
	t.Helper()
	data, err := os.ReadFile("testdata/events.json")
	if err != nil {
		t.Fatalf("LoadFixtures: read testdata/events.json: %v", err)
	}
	var f Fixtures
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatalf("LoadFixtures: unmarshal: %v", err)
	}
	return f
}

// ValidatedEvent returns the named fixture as a roots.ValidatedEvent.
// Calls t.Fatal if the key is missing or the event fails validation.
func (f Fixtures) ValidatedEvent(t *testing.T, key string) roots.ValidatedEvent {
	t.Helper()
	fe, ok := f.Events[key]
	if !ok {
		t.Fatalf("Fixtures.ValidatedEvent: key %q not found", key)
	}
	ve, err := roots.NewValidatedEvent(fe.Event)
	if err != nil {
		t.Fatalf("Fixtures.ValidatedEvent: key %q: %v", key, err)
	}
	return ve
}
