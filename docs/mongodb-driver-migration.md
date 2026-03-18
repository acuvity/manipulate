# MongoDB driver v2 migration notes

`manipmongo` now runs on `go.mongodb.org/mongo-driver/v2` instead of `github.com/globalsign/mgo`.

This is **not** just an internal dependency swap. If your code uses Mongo-specific
helpers, sharder implementations, raw database access, or BSON helper types, you
should treat this as a **breaking migration** and update downstream code accordingly.

## What stayed the same

- `manipmongo.New(...)` remains the constructor entry point.
- The main `Manipulator` and `TransactionalManipulator` usage patterns stay the same.
- CRUD operations still go through the same `Create`, `Retrieve`, `RetrieveMany`, `Update`, `Delete`, `DeleteMany`, and `Count` methods.

## Breaking changes you should expect

Downstream code will need changes if it uses any of the following:

- `CreateIndex(...)` now accepts `mongo.IndexModel`
- `EnsureIndex(...)` now accepts `mongo.IndexModel`
- `CreateCollection(...)` now accepts official-driver collection options builders
- `GetDatabase(...)` now returns `*mongo.Database`
- `OptionForceReadFilter(...)` now accepts official-driver `bson.D`
- `ContextOptionUpsert(...)` now accepts official-driver `bson.M`
- `Sharder.FilterOne(...)` / `Sharder.FilterMany(...)` now return official-driver `bson.D`
- model fields and helpers that used legacy `mgo/bson` types such as `bson.ObjectId` must move to official-driver BSON types such as `bson.ObjectID`

## What changed

The migration keeps the same helper names where possible, but some helper signatures now use official-driver types because the old `mgo` types are no longer available.

### Index helpers

```go
// old
manipmongo.CreateIndex(m, identity, mgo.Index{Key: []string{"name"}})

// new
manipmongo.CreateIndex(m, identity, mongo.IndexModel{
    Keys: bson.D{{Key: "name", Value: 1}},
})
```

```go
// old
manipmongo.EnsureIndex(m, identity, mgo.Index{Key: []string{"name"}})

// new
manipmongo.EnsureIndex(m, identity, mongo.IndexModel{
    Keys: bson.D{{Key: "name", Value: 1}},
})
```

### Collection creation

```go
// old
manipmongo.CreateCollection(m, identity, &mgo.CollectionInfo{})

// new
manipmongo.CreateCollection(m, identity, mongooptions.CreateCollection())
```

### Direct database access

```go
// old
mdb, closeFn, err := manipmongo.GetDatabase(m)
defer closeFn()

// new
mdb, err := manipmongo.GetDatabase(m)
```

`GetDatabase(...)` now returns an official-driver `*mongo.Database`.
It is derived from the manipmongo instance's **current** default read/write consistency,
including any overrides applied through `SetConsistencyMode(...)`.

There is no per-call close function anymore because the returned database handle
shares the manipulator's underlying official-driver client. If you need to shut
down the manipulator's client lifecycle explicitly, call:

```go
err := manipmongo.Disconnect(m, ctx)
```

When you use `GetDatabase(...)` for raw driver calls, you must pass your own
`context.Context` with the deadline/cancellation behavior you want. The
`OptionSocketTimeout(...)` manipmongo setting only applies to built-in
`Create`/`Retrieve`/`RetrieveMany`/`Update`/`Delete`/`DeleteMany`/`Count` paths;
it is **not** injected into raw official-driver operations done through
`GetDatabase(...)`.

`RunQuery(...)` continues to be the helper for raw driver operations, but it is
intentionally conservative about caller-owned contexts: plain
`context.Canceled` and `context.DeadlineExceeded` returned by your callback are
**not** retried automatically.

### Consistency mode helper

```go
// old
manipmongo.SetConsistencyMode(m, mgo.Monotonic, true)

// new
err := manipmongo.SetConsistencyMode(
    m,
    manipulate.ReadConsistencyMonotonic,
    manipulate.WriteConsistencyDefault,
)
```

`ReadConsistencyMonotonic` is still available, but it now maps to the
official driver's `PrimaryPreferred` read preference because `mgo`'s
monotonic mode has no exact equivalent in `mongo-driver/v2`.

`ReadConsistencyEventual` now maps to the official driver's `Nearest` read
preference. In `mgo`, eventual mode was effectively nearest-like but without
session stickiness across sequential reads; the official driver does not expose
that stickiness behavior as a read preference, so `Nearest` is the closest
semantic match. `ReadConsistencyWeakest` continues to map to
`SecondaryPreferred`.

Use a non-default write consistency only if you intentionally want to change the
manipulator's default write concern as part of the migration. Passing
`ReadConsistencyDefault` and/or `WriteConsistencyDefault` to `SetConsistencyMode(...)`
now means **leave the existing default unchanged** for that side. The old
`SetConsistencyMode(m, mgo.Monotonic, true)` call pattern was typically about
read behavior, not about changing write durability.

For clarity, admin/helper operations such as `CreateCollection(...)`,
`CreateIndex(...)`, `EnsureIndex(...)`, `DeleteIndex(...)`, and
`DropDatabase(...)` always use acknowledged writes in the migrated code,
regardless of any default write consistency override. `GetDatabase(...)`
continues to return a handle derived from the current default read/write
consistency.

### BSON imports

If your code imports legacy BSON types, switch to the official driver path:

```go
import bson "go.mongodb.org/mongo-driver/v2/bson"
```

That applies to helper arguments such as `OptionForceReadFilter(...)` and `ContextOptionUpsert(...)`.
It also applies to model fields and identifier helpers that previously used legacy
`mgo/bson` types such as `bson.ObjectId`; those should move to the official-driver
`bson.ObjectID` equivalents.

### `OptionForceReadFilter(...)` before/after

```go
// old
manipmongo.OptionForceReadFilter(mgobson.D{{Name: "tenant", Value: "acuvity"}})

// new
manipmongo.OptionForceReadFilter(bson.D{{Key: "tenant", Value: "acuvity"}})
```

### `ContextOptionUpsert(...)` before/after

```go
// old
manipulate.NewContext(
    ctx,
    manipmongo.ContextOptionUpsert(mgobson.M{
        "$setOnInsert": mgobson.M{"created": time.Now()},
    }),
)

// new
manipulate.NewContext(
    ctx,
    manipmongo.ContextOptionUpsert(bson.M{
        "$setOnInsert": bson.M{"created": time.Now()},
    }),
)
```

### Sharder interface changes

`Sharder.FilterOne(...)` and `Sharder.FilterMany(...)` now return official-driver `bson.D` values.

```go
// old
func (s *mySharder) FilterOne(
    manip manipulate.TransactionalManipulator,
    mctx manipulate.Context,
    obj elemental.Identifiable,
) (mgobson.D, error) {
    return mgobson.D{{Name: "tenant", Value: "acuvity"}}, nil
}

func (s *mySharder) FilterMany(
    manip manipulate.TransactionalManipulator,
    mctx manipulate.Context,
    identity elemental.Identity,
) (mgobson.D, error) {
    return mgobson.D{{Name: "tenant", Value: "acuvity"}}, nil
}

// new
func (s *mySharder) FilterOne(
    manip manipulate.TransactionalManipulator,
    mctx manipulate.Context,
    obj elemental.Identifiable,
) (bson.D, error) {
    return bson.D{{Key: "tenant", Value: "acuvity"}}, nil
}

func (s *mySharder) FilterMany(
    manip manipulate.TransactionalManipulator,
    mctx manipulate.Context,
    identity elemental.Identity,
) (bson.D, error) {
    return bson.D{{Key: "tenant", Value: "acuvity"}}, nil
}
```

## Timeout behavior is now split explicitly

Legacy `mgo` socket timeout semantics do not map cleanly to the official driver.
`manipmongo` now exposes the two relevant timeout behaviors separately:

- `OptionSocketTimeout(...)`: sets the default **per-operation** timeout used by manipmongo when the provided `manipulate.Context` has no deadline.
- `OptionClientTimeout(...)`: sets the official driver's **client-wide** timeout.

Example:

```go
m, err := manipmongo.New(
    uri,
    db,
    manipmongo.OptionConnectionTimeout(10*time.Second),
    manipmongo.OptionSocketTimeout(60*time.Second),   // per operation default
    manipmongo.OptionClientTimeout(0),                // leave client-wide timeout unset
)
```

If you previously relied on `OptionSocketTimeout(...)`, review long-running operations
carefully and decide whether you want:

Raw official-driver calls made from a handle returned by `GetDatabase(...)`
must set their own contexts/deadlines explicitly. `OptionSocketTimeout(...)`
does not wrap those raw calls for you.

- only a per-operation default,
- only an official-driver client timeout,
- both,
- or neither.

## Review checklist for downstream callers

- replace `mgo.Index` with `mongo.IndexModel`
- replace `*mgo.CollectionInfo` with `mongooptions.CreateCollection()` options
- update direct database usage to official-driver APIs and set explicit contexts/deadlines for raw driver calls
- switch BSON imports to `go.mongodb.org/mongo-driver/v2/bson`
- update sharder implementations to return official-driver `bson.D`
- review uses of `OptionForceReadFilter(...)` and `ContextOptionUpsert(...)`
- review timeout configuration and decide between `OptionSocketTimeout(...)` and `OptionClientTimeout(...)`
