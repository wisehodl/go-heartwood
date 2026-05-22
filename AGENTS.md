# go-heartwood

## Testing
- Baseline: `go test -count=1 ./...`
- Do not use `-race`: `boltdb/bolt@v1.3.1` triggers a `checkptr` panic under race instrumentation; this is an upstream bug, not a project defect.
