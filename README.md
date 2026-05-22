# Go-Heartwood - Nostr Event Storage for Neo4j

Source: https://git.wisehodl.dev/jay/go-heartwood

Mirror: https://github.com/wisehodl/go-heartwood

> **This library is in active development and not yet ready for use.**

`go-heartwood` is a Neo4j-based event storage layer for Nostr events. It stores events as nodes in a graph and foreign-key references as relationships.

## Installation

Add `go-heartwood` to your project:

```bash
go get git.wisehodl.dev/jay/go-heartwood
```

If the primary repository is unavailable, use the `replace` directive in your go.mod file to get the package from the github mirror:

```
replace git.wisehodl.dev/jay/go-heartwood => github.com/wisehodl/go-heartwood latest
```

## Usage

*Further usage examples will be added as the API stabilizes.*

### Writing Events

```go
package main

import (
	"context"
	"log"
	"time"

	"git.wisehodl.dev/jay/go-roots/events"
	"git.wisehodl.dev/jay/go-roots/keys"
	"git.wisehodl.dev/jay/go-heartwood"

	"github.com/boltdb/bolt"
)

func main() {
	ctx := context.Background()

	// Connect to Neo4j
	driver, err := heartwood.ConnectNeo4j(ctx, "bolt://localhost:7687", "neo4j", "password")
	if err != nil {
		log.Fatal(err)
	}
	defer driver.Close(ctx)

	// Ensure the necessary indexes and constraints exist
	if err := heartwood.SetNeo4jSchema(ctx, driver); err != nil {
		log.Fatal(err)
	}

	// Open BoltDB
	boltdb, err := bolt.Open("events.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer boltdb.Close()

	// Build and validate an event using go-roots
	sk, _ := keys.GeneratePrivateKey()
	pk, _ := keys.GetPublicKey(sk)

	event := events.NewEvent(
		events.WithPubKey(pk),
		events.WithCreatedAt(time.Now().Unix()),
		events.WithKind(1),
		events.WithContent("hello from heartwood"),
	)
	event.ID = events.GetID(event)
	sig, err := events.SignEvent(event.ID, sk)
	if err != nil {
		log.Fatal(err)
	}
	event.Sig = sig

	validated, err := events.NewValidatedEvent(event)
	if err != nil {
		log.Fatal(err)
	}

	// Write events
	report := heartwood.WriteEvents(
		[]events.ValidatedEvent{validated},
		driver, boltdb,
		nil, // default WriteOptions
	)
	if report.Error != nil {
		log.Fatal(report.Error)
	}

	log.Printf("created: %d, excluded: %d, duration: %s",
		report.CreatedEventCount,
		len(report.ExcludedEvents),
		report.Duration,
	)
}
```

## Testing

```bash
go test ./...
```
