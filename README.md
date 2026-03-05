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

*Usage examples will be added as the API stabilizes.*

## Testing

Run tests with:

```bash
go test ./...
```

Run with race detector:

```bash
go test -race ./...
```
