# Mongo Driver Signature Migration Checklist

Use this checklist when migrating downstream callers from the legacy `mgo`-backed
`manipmongo` API to the official `go.mongodb.org/mongo-driver/v2` API surface.

## How To Use This Doc

For each consumer repository:

1. Find usages of each API below.
2. Replace the old signature/type with the new one.
3. Check the box only after the code compiles and the service-specific tests pass.

## Public Signature Changes

### Constructor Contract

- [ ] `manipmongo.New(...)` is the only exported constructor in the new version.
  - Old usage pattern: legacy callers may still assume legacy helper constructors or legacy runtime semantics.
  - New usage pattern:
    ```go
    manipulator, err := manipmongo.New(uri, dbName, options...)
    ```
  - Notes:
    - Constructor still returns `(manipulate.TransactionalManipulator, error)`.
    - Runtime behavior is now backed by the official Mongo driver.

### `CreateIndex`

- [ ] Replace `mgo.Index` with `mongo.IndexModel`.
  ```go
  // Old
  func CreateIndex(manipulator manipulate.Manipulator, identity elemental.Identity, indexes ...mgo.Index) error

  // New
  func CreateIndex(manipulator manipulate.Manipulator, identity elemental.Identity, indexes ...mongo.IndexModel) error
  ```
  - Caller action:
    - Import `go.mongodb.org/mongo-driver/v2/mongo`.
    - Rewrite index definitions to `mongo.IndexModel`.
  - Example:
    ```go
    err := manipmongo.CreateIndex(
        manipulator,
        identity,
        mongo.IndexModel{
            Keys: bson.D{{Key: "name", Value: 1}},
            Options: options.Index().SetName("idx_name"),
        },
    )
    ```

### `EnsureIndex`

- [ ] Replace `mgo.Index` with `mongo.IndexModel`.
  ```go
  // Old
  func EnsureIndex(manipulator manipulate.Manipulator, identity elemental.Identity, indexes ...mgo.Index) error

  // New
  func EnsureIndex(manipulator manipulate.Manipulator, identity elemental.Identity, indexes ...mongo.IndexModel) error
  ```
  - Caller action:
    - Same migration pattern as `CreateIndex`.

### `CreateCollection`

- [ ] Replace `*mgo.CollectionInfo` with official-driver create options.
  ```go
  // Old
  func CreateCollection(manipulator manipulate.Manipulator, identity elemental.Identity, info *mgo.CollectionInfo) error

  // New
  func CreateCollection(
      manipulator manipulate.Manipulator,
      identity elemental.Identity,
      opts ...mongooptions.Lister[mongooptions.CreateCollectionOptions],
  ) error
  ```
  - Caller action:
    - Import `go.mongodb.org/mongo-driver/v2/mongo/options`.
    - Convert collection metadata into official-driver option builders.

### `GetDatabase`

- [ ] Update return handling from legacy database-plus-close-callback to official-driver database only.
  ```go
  // Old
  func GetDatabase(manipulator manipulate.Manipulator) (*mgo.Database, func(), error)

  // New
  func GetDatabase(manipulator manipulate.Manipulator) (*mongo.Database, error)
  ```
  - Caller action:
    - Remove the returned close callback handling.
    - Update all direct DB/collection operations to official-driver APIs.

### `SetConsistencyMode`

- [ ] Replace `mgo.Mode`/`refresh` usage with `manipulate` read/write consistency enums.
  ```go
  // Old
  func SetConsistencyMode(manipulator manipulate.Manipulator, mode mgo.Mode, refresh bool)

  // New
  func SetConsistencyMode(
      manipulator manipulate.Manipulator,
      readConsistency manipulate.ReadConsistency,
      writeConsistency manipulate.WriteConsistency,
  ) error
  ```
  - Caller action:
    - Replace direct `mgo.Mode` values with `manipulate.ReadConsistency*`.
    - Set write semantics separately with `manipulate.WriteConsistency*`.
    - Handle returned `error`.

## Same-Shape APIs With Type-Path Changes

These functions keep the same high-level signature shape but switch from
`github.com/globalsign/mgo/bson` types to `go.mongodb.org/mongo-driver/v2/bson`
types.

### `OptionForceReadFilter`

- [ ] Replace legacy `bson.D` imports with official-driver `bson.D`.
  ```go
  // Old
  func OptionForceReadFilter(f mgoBson.D) Option

  // New
  func OptionForceReadFilter(f mongoBson.D) Option
  ```

### `ContextOptionUpsert`

- [ ] Replace legacy `bson.M` imports with official-driver `bson.M`.
  ```go
  // Old
  func ContextOptionUpsert(operations mgoBson.M) manipulate.ContextOption

  // New
  func ContextOptionUpsert(operations mongoBson.M) manipulate.ContextOption
  ```

### `Sharder` Interface

- [ ] Update `FilterOne` and `FilterMany` to return official-driver `bson.D`.
  ```go
  // Old effective contract
  FilterOne(...) (mgoBson.D, error)
  FilterMany(...) (mgoBson.D, error)

  // New contract
  FilterOne(...) (mongoBson.D, error)
  FilterMany(...) (mongoBson.D, error)
  ```

### `internal/objectid.Parse`

- [ ] Update downstream expectations from `bson.ObjectId` to `bson.ObjectID`.
  ```go
  // Old
  func Parse(s string) (bson.ObjectId, bool)

  // New
  func Parse(s string) (bson.ObjectID, bool)
  ```

## New Helper APIs

- [ ] Use `ContextOptionUpsertSafe` where callers want validation errors instead of panic.
  ```go
  func ContextOptionUpsertSafe(operations bson.M) (manipulate.ContextOption, error)
  ```

- [ ] Use safe attribute-encrypter helpers when callers need typed errors instead of panic.
  ```go
  func SetAttributeEncrypterSafe(manipulator manipulate.Manipulator, enc elemental.AttributeEncrypter) error
  func GetAttributeEncrypterSafe(manipulator manipulate.Manipulator) (elemental.AttributeEncrypter, error)
  ```

## No External Signature Change, Internal Runtime Changed

These methods moved to the official-driver runtime implementation, but callers
invoke them the same way through the `manipulate.TransactionalManipulator`
interface:

- [ ] `RetrieveMany`
- [ ] `Retrieve`
- [ ] `Create`
- [ ] `Update`
- [ ] `Delete`
- [ ] `DeleteMany`
- [ ] `Count`
- [ ] `Commit`
- [ ] `Abort`
- [ ] `Ping`

Use this section as a reminder to run behavioral smoke tests even when compile
errors do not occur.

## Per-Service Validation

- [ ] `go mod tidy`
- [ ] Service build passes
- [ ] Service unit/integration tests pass
- [ ] Mongo-sensitive smoke checks pass
- [ ] Direct database helper usage (`GetDatabase`, index helpers, collection helpers) is validated
- [ ] Rollback path to prior `manipulate` version is documented
