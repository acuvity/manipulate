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
ctx := manipulate.NewContextWithFilter(elemental.NewFilterComposer().
    WithKey("login").Equals("primalmotion").
    Done(),
)

// Retrieve the users matching the filter.
var users models.UserLists
if err := m.RetrieveMany(ctx, &users); err != nil {
    return err
}
```
