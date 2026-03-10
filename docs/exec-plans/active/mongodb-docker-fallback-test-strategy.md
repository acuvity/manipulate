# Docker-First Mongo Test Fallback Plan (feature/mongodb-migration)

## Summary
Adopt a Docker-first Mongo integration path for `manipmongo` tests with deterministic fallback behavior:
1. `MONGO_TEST_URI` override first.
2. Auto-start local Docker Mongo second.
3. If unavailable, skip Mongo-dependent integration tests (mock/unit tests still run).
4. If strict mode is enabled, fail instead of skipping.

## Test Interface Contract
- `MONGO_TEST_URI`: optional; if set and reachable, tests use it directly.
- `MONGO_TEST_DOCKER_IMAGE`: optional; default `mongo:latest`.
- `MONGO_TEST_DOCKER_STARTUP_TIMEOUT`: optional duration; default `60s`.
- `MONGO_TEST_DOCKER_DISABLE`: optional bool; if true, skip Docker startup attempt.
- Keep existing strict toggle `REQUIRE_MEMONGO` for backward compatibility.
- Add alias `REQUIRE_MONGO` with identical semantics.

Strict mode behavior:
- If strict is true, failure to acquire Mongo endpoint is `t.Fatalf`.
- Otherwise Mongo-dependent tests are skipped via `t.Skipf`.

## Implementation
1. Keep existing `requireMemongo(t)` call sites unchanged.
2. Implement endpoint resolution order in test util:
   - Try `MONGO_TEST_URI` and ping.
   - Else try Docker Mongo bootstrap and ping.
   - Else skip/fail via strict gate.
3. Reuse per-test DB naming and cleanup (`DropDatabaseByURI`).
4. Ensure `TestMain` cleans up Docker container if started.
5. Include clear reason strings for fallback decisions.

## Validation Matrix
- `go test ./manipmongo/...` with Docker available.
- `MONGO_TEST_DOCKER_DISABLE=1 go test ./manipmongo/...` verifies skip fallback.
- `REQUIRE_MONGO=1 MONGO_TEST_DOCKER_DISABLE=1 go test ./manipmongo/...` verifies strict fail mode.
- `go test ./...` verifies no cross-package regressions.

## Risks
- `mongo:latest` may change over time; allow pinning with `MONGO_TEST_DOCKER_IMAGE`.
- Docker startup can add latency; use one container per test process.
