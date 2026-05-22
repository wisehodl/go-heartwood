# go-heartwood

## Agent Skills
- Skills are located in `~/.pi/agent/skills/`
- Skill scripts are located in their respective skill directories under `scripts/` -- look for them there, not in this repository.

## Testing
- Baseline: `go test -count=1 ./...`
- Do not use `-race`: `boltdb/bolt@v1.3.1` triggers a `checkptr` panic under race instrumentation; this is an upstream bug, not a project defect.
