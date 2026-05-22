// gen-fixtures generates cryptographically valid Nostr test events and writes
// them to testdata/events.json. The three private keys are hardcoded so
// subsequent runs produce identical output.
//
// Usage: go run ./cmd/gen-fixtures
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	roots "git.wisehodl.dev/jay/go-roots/events"
	"git.wisehodl.dev/jay/go-roots/keys"
)

// Hardcoded private keys — do not change; output must be deterministic.
const (
	alicePrivKey = "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1"
	bobPrivKey   = "b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2"
	carolPrivKey = "c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3"
)

type fixtureEvent struct {
	Description string     `json:"description"`
	Event       roots.Event `json:"event"`
}

type fixtures struct {
	Events map[string]fixtureEvent `json:"events"`
	Keys   map[string]string       `json:"keys"`
}

func mustPubKey(privKey string) string {
	pk, err := keys.GetPublicKey(privKey)
	if err != nil {
		panic(fmt.Sprintf("GetPublicKey: %v", err))
	}
	return pk
}

func mustSign(e roots.Event, privKey string) roots.Event {
	id := roots.GetID(e)
	e.ID = id
	sig, err := roots.SignEvent(id, privKey)
	if err != nil {
		panic(fmt.Sprintf("SignEvent: %v", err))
	}
	e.Sig = sig
	return e
}

func build(privKey string, opts ...roots.EventOption) roots.Event {
	pk := mustPubKey(privKey)
	base := []roots.EventOption{roots.WithPubKey(pk), roots.WithCreatedAt(1000)}
	e := roots.NewEvent(append(base, opts...)...)
	return mustSign(e, privKey)
}

func main() {
	alicePub := mustPubKey(alicePrivKey)
	bobPub := mustPubKey(bobPrivKey)

	// carol_placeholder must be built first; its ID is used as an e-tag value.
	carolPlaceholder := build(carolPrivKey,
		roots.WithKind(1),
		roots.WithContent("carol placeholder"),
	)

	f := fixtures{
		Events: map[string]fixtureEvent{
			"carol_placeholder": {
				Description: "Provides real event ID for e-tag tests",
				Event:       carolPlaceholder,
			},
			"bare": {
				Description: "Minimal event, no tags",
				Event: build(alicePrivKey,
					roots.WithKind(1),
					roots.WithContent("bare"),
				),
			},
			"generic_tag": {
				Description: "Generic t-tag; no expander triggered",
				Event: build(alicePrivKey,
					roots.WithKind(1),
					roots.WithContent("generic tag"),
					roots.WithTag(roots.Tag{"t", "bitcoin"}),
				),
			},
			"e_tag_valid": {
				Description: "e-tag referencing carol_placeholder.id; triggers ExpandTaggedEvents",
				Event: build(alicePrivKey,
					roots.WithKind(1),
					roots.WithContent("e tag valid"),
					roots.WithTag(roots.Tag{"e", carolPlaceholder.ID}),
				),
			},
			"e_tag_invalid": {
				Description: "e-tag with non-hex value; expander skips it",
				Event: build(alicePrivKey,
					roots.WithKind(1),
					roots.WithContent("e tag invalid"),
					roots.WithTag(roots.Tag{"e", "notvalid"}),
				),
			},
			"p_tag_valid": {
				Description: "p-tag referencing bob's pubkey; triggers ExpandTaggedUsers",
				Event: build(alicePrivKey,
					roots.WithKind(1),
					roots.WithContent("p tag valid"),
					roots.WithTag(roots.Tag{"p", bobPub}),
				),
			},
			"p_tag_invalid": {
				Description: "p-tag with non-hex value; expander skips it",
				Event: build(alicePrivKey,
					roots.WithKind(1),
					roots.WithContent("p tag invalid"),
					roots.WithTag(roots.Tag{"p", "notvalid"}),
				),
			},
			"replaceable_k0": {
				Description: "Kind 0; triggers ExpandReplaceableEvents",
				Event: build(alicePrivKey,
					roots.WithKind(0),
					roots.WithContent("replaceable k0"),
				),
			},
			"replaceable_k3": {
				Description: "Kind 3; triggers ExpandReplaceableEvents",
				Event: build(alicePrivKey,
					roots.WithKind(3),
					roots.WithContent("replaceable k3"),
				),
			},
			"replaceable_k10k": {
				Description: "Kind 10002; triggers ExpandReplaceableEvents",
				Event: build(alicePrivKey,
					roots.WithKind(10002),
					roots.WithContent("replaceable k10k"),
				),
			},
		},
		Keys: map[string]string{
			"alice": alicePub,
			"bob":   bobPub,
			"carol": mustPubKey(carolPrivKey),
		},
	}

	// Resolve output path relative to project root (two levels up from this file).
	_, thisFile, _, _ := runtime.Caller(0)
	projectRoot := filepath.Join(filepath.Dir(thisFile), "..", "..")
	outPath := filepath.Join(projectRoot, "testdata", "events.json")

	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal: %v\n", err)
		os.Exit(1)
	}

	if err := os.WriteFile(outPath, data, 0644); err != nil {
		fmt.Fprintf(os.Stderr, "write: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("wrote %s\n", outPath)
}
