# Mongo Driver Migration (Clean-Cut Versioned Rollout)

## Summary

Migrate `manipmongo` to `go.mongodb.org/mongo-driver/v2` in a clean new release with no legacy compatibility bridges.
Rollout uses version split: services not yet migrated stay on the previous `manipulate` version; migrated services move to the new version and update API usage as needed.
Public constructor for consumers remains `manipmongo.New(...)` only.

## Scope

- In:
  - Make `manipmongo.New(...)` use the official Mongo driver in the new release.
  - Remove legacy bridge behavior and `mgo`-typed signatures from the new release to keep the API surface clean.
  - Keep current module path and publish explicit migration notes.
  - Migrate consuming services one-by-one (not a one-shot cutover).
  - Treat `backend-tutorial` as low-priority sample code.
- Out:
  - Forcing all services to migrate in one release window.
  - Re-introducing compatibility bridges in the new version.
  - Unrelated service refactors during migration PRs.

## Target Public API Contract (New Version)

- Constructor:
  - `manipmongo.New(...)` is the official driver constructor.
- Canonical API names are retained in the new version (breaking signature/type updates):
  - `GetDatabase(...)` now returns official-driver database (`*mongo.Database`) instead of legacy `*mgo.Database`.
  - `EnsureIndex(...)` and `CreateIndex(...)` now accept official-driver index models (for example `mongo.IndexModel`) instead of legacy `mgo.Index`.
  - `CreateCollection(...)` now uses official-driver create-collection options instead of `*mgo.CollectionInfo`.
  - `OptionSharder(...)` now expects `Sharder`, the canonical official-driver sharder interface.
  - `OptionForceReadFilter(...)` now expects official-driver `bson.D`.
  - `ContextOptionUpsert(...)` now expects official-driver `bson.M`.
  - `IsMongoManipulator(...)` remains the canonical runtime type check and must return `true` for the official-driver manipulator.
  - Legacy adapters used only for bridge behavior are removed.
- Redundant public `*Mongo` aliases/helpers are not part of the target release surface.
  - Do not keep parallel exported helpers such as `GetMongoDatabase(...)`, `SetMongoConsistencyMode(...)`, or `IsMongoDriverManipulator(...)` when the same-name canonical helper already exists.
  - Do not keep exported legacy-placeholder errors that only describe removed bridge behavior.
  - Do not keep a parallel exported `SharderMongo` split when `Sharder` is the canonical public interface.

Detailed caller-facing signature deltas are tracked in:
- [`mongodb-driver-signature-migration-checklist.md`](./mongodb-driver-signature-migration-checklist.md)

## Migration Principles

1. Version split is the safety mechanism.
   - Non-migrated services pin old `manipulate` version.
   - Migrated services consume the new version.
2. Keep service PRs narrow.
   - Only dependency bump + required API migrations.
3. Validate at package and service boundaries.
   - Use `manipmongo` package-focused test gates before downstream rollout.

## Dependency Graph

- T1: Audit current feature branch and classify existing changes as `keep`, `drop`, or `defer` under clean-cut strategy. `depends_on: []`
- T2: Lock breaking API contract and same-name signature mapping for new version docs/release notes, including same-name helper parity (`IsMongoManipulator(...)`) and removal of redundant `*Mongo` exported aliases. `depends_on: [T1]`
- T3: Update `manipmongo.New(...)` as official-driver path and remove bridge-only code/APIs from exported surface. `depends_on: [T2]`
- T4: Update docs/examples to `New(...)`-only contract and same-name signature migrations (README + migration notes). `depends_on: [T2, T3]`
- T5: Validate `manipulate` gates (`go test ./manipmongo/...`, coverage gate, Mongo matrix). `depends_on: [T3]`
- T6: Release new `manipulate` tag (current module path) with explicit breaking-change notes and migration guide. `depends_on: [T4, T5]`
- T7: Migrate `a3s` to new version and replace legacy API usage with official-driver APIs. `depends_on: [T6]`
- T8: Migrate `acuvity/backend` mongo-heavy services one-by-one (`lain` -> `avi` -> `zerolift`) on new version. `depends_on: [T6, T7]`
- T9: Migrate remaining direct consumers with version bump, applying code changes only if compile/runtime breakage occurs. `depends_on: [T6]`
- T10: Migrate `backend-tutorial` as low-priority sample after production services stabilize. `depends_on: [T8, T9]`
- T11: Track holdouts pinned to old version and define legacy-version retirement checkpoint. `depends_on: [T8, T9]`

## Service Migration Order

1. `a3s` (highest legacy API usage)
2. `acuvity/backend` services with direct `manipmongo` usage
   - `lain`
   - `avi`
   - `zerolift`
3. Remaining direct consumers (`acucheck`, `aculib`, `acusysd`, `infra/acute`, `maxibridge`, `otelexporter`, `marcus-playground/otel-receiver`, and non-`manipmongo` backend services)
4. `backend-tutorial` (low priority)

## Per-Service Migration Checklist

1. Bump `go.acuvity.ai/manipulate` to new migration tag.
2. Update same-name API calls to the new official-driver signatures/types.
3. Run `go mod tidy`.
4. Run service test/build gates.
5. Run service smoke checks for Mongo-sensitive paths.
6. Record outcome in rollout tracker (pass/fail, blocker, rollback command).

Use the detailed API-by-API migration checklist here:
- [`mongodb-driver-signature-migration-checklist.md`](./mongodb-driver-signature-migration-checklist.md)

## Rollback Model

- Rollback is version-based, not bridge-based.
- If a migrated service regresses:
  1. Revert that service to previous `manipulate` tag.
  2. Keep other services unchanged on their current versions.

## Gate Criteria

1. New `manipulate` release passes package-level gates and Mongo matrix validation.
2. `a3s` and `acuvity/backend` mongo-heavy services pass migration validation.
3. Remaining direct consumers are either migrated successfully or explicitly tracked as old-version holdouts.
4. No requirement for one-shot org-wide migration.
