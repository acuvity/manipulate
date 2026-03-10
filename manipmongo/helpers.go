// Copyright 2019 Aporeto Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package manipmongo

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.acuvity.ai/elemental"
	"go.acuvity.ai/manipulate"
	"go.acuvity.ai/manipulate/internal/backoff"
	odbson "go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mongooptions "go.mongodb.org/mongo-driver/v2/mongo/options"
)

// DoesDatabaseExist checks if the database used by the given manipulator exists.
func DoesDatabaseExist(manipulator manipulate.Manipulator) (bool, error) {

	m, err := mongoDriverManipulatorFromAPI(manipulator)
	if err != nil {
		return false, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultGlobalContextTimeout)
	defer cancel()

	dbs, err := m.client.ListDatabaseNames(ctx, odbson.D{})
	if err != nil {
		return false, err
	}

	for _, db := range dbs {
		if db == m.dbName {
			return true, nil
		}
	}

	return false, nil
}

// DropDatabase drops the entire database used by the given manipulator.
func DropDatabase(manipulator manipulate.Manipulator) error {

	m, err := mongoDriverManipulatorFromAPI(manipulator)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultGlobalContextTimeout)
	defer cancel()
	return m.client.Database(m.dbName).Drop(ctx)
}

// CreateIndex creates indexes for the collection storing info for the given
// identity using official-driver index models.
func CreateIndex(manipulator manipulate.Manipulator, identity elemental.Identity, indexes ...mongo.IndexModel) error {
	return createIndexesWithContext(context.Background(), manipulator, identity, indexes...)
}

// EnsureIndex creates indexes, dropping/recreating conflicting indexes to
// mirror legacy EnsureIndex behavior, using official-driver index models.
func EnsureIndex(manipulator manipulate.Manipulator, identity elemental.Identity, indexes ...mongo.IndexModel) error {
	return ensureIndexesWithContext(context.Background(), manipulator, identity, indexes...)
}

// DeleteIndex drops indexes by name for the collection.
func DeleteIndex(manipulator manipulate.Manipulator, identity elemental.Identity, indexes ...string) error {

	m, err := mongoDriverManipulatorFromAPI(manipulator)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultGlobalContextTimeout)
	defer cancel()

	indexView := m.client.Database(m.dbName).Collection(identity.Name).Indexes()
	for _, index := range indexes {
		if err := indexView.DropOne(ctx, index); err != nil {
			return err
		}
	}

	return nil
}

// CreateCollection creates a collection using official mongo driver options.
func CreateCollection(
	manipulator manipulate.Manipulator,
	identity elemental.Identity,
	opts ...mongooptions.Lister[mongooptions.CreateCollectionOptions],
) error {
	return createCollectionWithContext(context.Background(), manipulator, identity, opts...)
}

// createIndexesWithContext creates indexes for the given identity using the
// official mongo-driver-backed manipulator and the provided context.
func createIndexesWithContext(
	ctx context.Context,
	manipulator manipulate.Manipulator,
	identity elemental.Identity,
	indexes ...mongo.IndexModel,
) error {

	m, err := mongoDriverManipulatorFromAPI(manipulator)
	if err != nil {
		return err
	}

	if len(indexes) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	_, err = m.client.Database(m.dbName).Collection(identity.Name).Indexes().CreateMany(ctx, indexes)
	if err != nil {
		return fmt.Errorf("unable to create indexes: %w", err)
	}

	return nil
}

// ensureIndexesWithContext creates indexes with conflict handling using the
// provided context for cancellation/deadline control.
func ensureIndexesWithContext(
	ctx context.Context,
	manipulator manipulate.Manipulator,
	identity elemental.Identity,
	indexes ...mongo.IndexModel,
) error {

	m, err := mongoDriverManipulatorFromAPI(manipulator)
	if err != nil {
		return err
	}

	if len(indexes) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	coll := m.client.Database(m.dbName).Collection(identity.Name)
	indexView := coll.Indexes()

	for i, index := range indexes {

		preparedIndex, idxName, expireAfter, err := normalizeMongoIndexModel(identity, i, index)
		if err != nil {
			return fmt.Errorf("unable to prepare index %d: %w", i, err)
		}

		_, err = indexView.CreateOne(ctx, preparedIndex)
		if err == nil {
			continue
		}

		if !isMongoIndexConflictError(err) {
			return fmt.Errorf("unable to ensure index '%s': %w", idxName, err)
		}

		// In case we are changing TTL we use collMod, same rationale as legacy EnsureIndex.
		if expireAfter != nil && *expireAfter > 0 {
			if err := coll.Database().RunCommand(ctx, odbson.D{
				{Key: "collMod", Value: coll.Name()},
				{Key: "index", Value: odbson.M{"name": idxName, "expireAfterSeconds": *expireAfter}},
			}).Err(); err != nil {
				return fmt.Errorf("cannot update TTL index '%s': %w", idxName, err)
			}
			continue
		}

		if err := indexView.DropOne(ctx, idxName); err != nil {
			return fmt.Errorf("cannot delete previous index '%s': %w", idxName, err)
		}
		if _, err := indexView.CreateOne(ctx, preparedIndex); err != nil {
			return fmt.Errorf("unable to ensure index after dropping old one '%s': %w", idxName, err)
		}
	}

	return nil
}

// createCollectionWithContext creates a collection using official mongo driver
// options and the provided context.
func createCollectionWithContext(
	ctx context.Context,
	manipulator manipulate.Manipulator,
	identity elemental.Identity,
	opts ...mongooptions.Lister[mongooptions.CreateCollectionOptions],
) error {

	m, err := mongoDriverManipulatorFromAPI(manipulator)
	if err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := m.client.Database(m.dbName).CreateCollection(ctx, identity.Name, opts...); err != nil {
		return fmt.Errorf("unable to create collection '%s': %w", identity.Name, err)
	}

	return nil
}

// GetDatabase returns a ready-to-use official-driver database object.
func GetDatabase(manipulator manipulate.Manipulator) (*mongo.Database, error) {
	m, err := mongoDriverManipulatorFromAPI(manipulator)
	if err != nil {
		return nil, err
	}

	return m.client.Database(m.dbName), nil
}

// SetConsistencyMode sets default read/write consistency used by
// the official-driver manipulator when contexts don't override it.
func SetConsistencyMode(
	manipulator manipulate.Manipulator,
	readConsistency manipulate.ReadConsistency,
	writeConsistency manipulate.WriteConsistency,
) error {
	m, err := mongoDriverManipulatorFromAPI(manipulator)
	if err != nil {
		return err
	}

	m.setDefaultConsistency(readConsistency, writeConsistency)
	return nil
}

// RunQuery runs a function that must run a mongodb operation.
// It will retry in case of failure.
func RunQuery(mctx manipulate.Context, operationFunc func() (any, error), baseRetryInfo RetryInfo) (any, error) {

	var try int

	for {

		out, err := operationFunc()
		if err == nil {
			return out, nil
		}

		err = HandleQueryError(err)
		if !manipulate.IsCannotCommunicateError(err) {
			return out, err
		}

		baseRetryInfo.try = try
		baseRetryInfo.err = err
		baseRetryInfo.mctx = mctx

		if rf := mctx.RetryFunc(); rf != nil {
			if rerr := rf(baseRetryInfo); rerr != nil {
				return nil, rerr
			}
		} else if baseRetryInfo.defaultRetryFunc != nil {
			if rerr := baseRetryInfo.defaultRetryFunc(baseRetryInfo); rerr != nil {
				return nil, rerr
			}
		}

		select {
		case <-mctx.Context().Done():
			return nil, manipulate.ErrCannotExecuteQuery{Err: mctx.Context().Err()}
		default:
		}

		deadline, _ := mctx.Context().Deadline()
		time.Sleep(backoff.NextWithCurve(try, deadline, defaultBackoffCurve))
		try++
	}
}

// SetAttributeEncrypter switch the attribute encrypter of the given mongo manipulator.
// This is only useful in some rare cases like miugration, and it is not go routine safe.
func SetAttributeEncrypter(manipulator manipulate.Manipulator, enc elemental.AttributeEncrypter) {

	m, ok := manipulator.(*mongoDriverManipulator)
	if !ok {
		panic("you can only pass a mongo manipulator to SetAttributeEncrypter")
	}

	m.attributeEncrypter = enc
}

// GetAttributeEncrypter returns the attribute encrypter of the given mongo manipulator..
func GetAttributeEncrypter(manipulator manipulate.Manipulator) elemental.AttributeEncrypter {

	m, ok := manipulator.(*mongoDriverManipulator)
	if !ok {
		panic("you can only pass a mongo manipulator to GetAttributeEncrypter")
	}

	return m.attributeEncrypter
}

// IsUpsert returns True if the mongo request is an Upsert operation, false otherwise.
func IsUpsert(mctx manipulate.Context) bool {
	_, upsert := mctx.(opaquer).Opaque()[opaqueKeyUpsert]
	return upsert
}

// IsMongoManipulator returns true if this is a mongo manipulator.
func IsMongoManipulator(manipulator manipulate.Manipulator) bool {
	_, ok := manipulator.(*mongoDriverManipulator)

	return ok
}

func mongoDriverManipulatorFromAPI(manipulator manipulate.Manipulator) (*mongoDriverManipulator, error) {

	m, ok := manipulator.(*mongoDriverManipulator)
	if !ok {
		return nil, fmt.Errorf("you can only pass a mongo manipulator: got %T", manipulator)
	}

	return m, nil
}

func normalizeMongoIndexModel(identity elemental.Identity, indexPos int, model mongo.IndexModel) (mongo.IndexModel, string, *int32, error) {

	if model.Options == nil {
		model.Options = mongooptions.Index()
	}

	applied, err := applyMongoIndexOptions(model.Options)
	if err != nil {
		return mongo.IndexModel{}, "", nil, err
	}

	idxName := ""
	if applied.Name != nil {
		idxName = *applied.Name
	}
	if idxName == "" {
		idxName = "index_" + identity.Name + "_" + strconv.Itoa(indexPos)
		model.Options.SetName(idxName)
	}

	return model, idxName, applied.ExpireAfterSeconds, nil
}

func applyMongoIndexOptions(builder *mongooptions.IndexOptionsBuilder) (*mongooptions.IndexOptions, error) {
	opts := &mongooptions.IndexOptions{}
	if builder == nil {
		return opts, nil
	}
	for _, apply := range builder.List() {
		if err := apply(opts); err != nil {
			return nil, err
		}
	}
	return opts, nil
}

func isMongoIndexConflictError(err error) bool {
	if err == nil {
		return false
	}

	var cmdErr mongo.CommandError
	if errors.As(err, &cmdErr) {
		switch cmdErr.Code {
		case 85, 86:
			return true
		}
	}

	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "already exists with different options") ||
		strings.Contains(lower, "indexoptionsconflict") ||
		strings.Contains(lower, "indexkeyspecsconflict") ||
		strings.Contains(lower, "is reserved for the _id index") ||
		strings.Contains(lower, "for an _id index specification")
}
