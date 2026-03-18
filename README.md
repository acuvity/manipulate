# Manipulate

[![Codacy Badge](https://app.codacy.com/project/badge/Grade/b5011051258243de99974313f4e4a8b6)](https://www.codacy.com/gh/PaloAltoNetworks/manipulate/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=PaloAltoNetworks/manipulate&amp;utm_campaign=Badge_Grade) [![Codacy Badge](https://app.codacy.com/project/badge/Coverage/b5011051258243de99974313f4e4a8b6)](https://www.codacy.com/gh/PaloAltoNetworks/manipulate/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=PaloAltoNetworks/manipulate&amp;utm_campaign=Badge_Coverage)

> Note: this readme is a work in progress

Package manipulate provides everything needed to perform CRUD operations on an
[elemental](https://go.acuvity.ai/elemental) based data model.

The main interface is `Manipulator`. This interface provides various methods for
creation, modification, retrieval and so on.

A Manipulator works with `elemental.Identifiable`.

The storage engine used by a Manipulator is abstracted. By default manipulate
provides implementations for Mongo, ReST HTTP 1and a Memory backed datastore.
You can of course implement Your own storage implementation.

There is also a mocking package called maniptest to make it easy to mock any
manipulator implementation for unit testing.

Each method of a Manipulator is taking a `manipulate.Context` as argument. The
context is used to pass additional informations like a Filter, or some
Parameters.

## MongoDB driver migration

`manipmongo` now uses `go.mongodb.org/mongo-driver/v2` internally. This is a
breaking migration for callers that use Mongo-specific helpers, BSON types,
raw driver handles, or custom sharders.

If you call helpers such as `CreateIndex`, `EnsureIndex`, `CreateCollection`,
`GetDatabase`, `OptionForceReadFilter`, or `ContextOptionUpsert`, see
[docs/mongodb-driver-migration.md](docs/mongodb-driver-migration.md) for the
required official-driver type changes and timeout migration guidance.

## MongoDB integration tests

The MongoDB integration suite lives under `manipmongo/...`.

For CI-style strict execution, use the Make targets:

```bash
make test-mongo
make test-mongo-cover
```

These Make targets are **CI-strict by default**: they set `REQUIRE_MONGO=1`, so
they fail instead of skip when MongoDB is unavailable.

If you want the non-strict local behavior, run the package tests directly:

```bash
go test ./manipmongo/...
```

Without strict mode, the tests will try, in order, to:

1. use `MONGO_TEST_URI` if it is set and reachable
2. provision a disposable Docker MongoDB instance
3. skip the test unless strict Mongo mode is requested

Useful environment variables:

- `MONGO_TEST_URI`: reuse an existing MongoDB instance instead of starting Docker
- `MONGO_TEST_DOCKER_IMAGE`: override the Docker image used for provisioning
- `MONGO_TEST_DOCKER_STARTUP_TIMEOUT`: override Docker startup wait time
- `MONGO_TEST_DOCKER_DISABLE=1`: disable Docker provisioning
- `REQUIRE_MONGO=1` or `REQUIRE_MEMONGO=1`: fail instead of skip when MongoDB is unavailable

The Make targets honor your existing Docker environment, including `DOCKER_HOST`.

Examples:

```bash
MONGO_TEST_URI=mongodb://127.0.0.1:27017 make test-mongo
MONGO_TEST_DOCKER_DISABLE=1 REQUIRE_MONGO=1 make test-mongo
```

## Example for creating an object

```go
// Create a User from a generated Elemental model.
user := models.NewUser() // always use the initializer to get various default values correctly set.
user.FullName = "Antoine Mercadal"
user.Login = "primalmotion"

// Create a Mongo manipulator.
m, err := manipmongo.New("mongodb://127.0.0.1:27017", "test")
if err != nil {
    return err
}

// Then create the User.
if err := m.Create(nil, user); err != nil {
    return err
}
```

## Example for retrieving an object

```go
// Create a Mongo manipulator.
m, err := manipmongo.New("mongodb://127.0.0.1:27017", "test")
if err != nil {
    return err
}

// Create a Context with a filter.
ctx := manipulate.NewContext(
    context.Background(),
    manipulate.ContextOptionFilter(
        elemental.NewFilterComposer().
            WithKey("login").Equals("primalmotion").
            Done(),
    ),
)

// Retrieve the users matching the filter.
var users models.UserLists
if err := m.RetrieveMany(ctx, &users); err != nil {
    return err
}
```
