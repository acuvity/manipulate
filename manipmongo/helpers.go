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

	m, ok := manipulator.(*mongoManipulator)
	if !ok {
		panic("you can only pass a mongo manipulator to DoesDatabaseExist")
	}

	ctx, cancel := contextWithDefaultTimeout(context.Background(), m.operationTimeout)
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
// It always uses acknowledged writes regardless of the manipulator's default
// write consistency override.
func DropDatabase(manipulator manipulate.Manipulator) error {

	m, ok := manipulator.(*mongoManipulator)
	if !ok {
		panic("you can only pass a mongo manipulator to DropDatabase")
	}

	ctx, cancel := contextWithDefaultTimeout(context.Background(), m.operationTimeout)
	defer cancel()

	return m.makeAcknowledgedDatabase().Drop(ctx)
}

// CreateIndex creates indexes for the collection storing info for the given identity using the given manipulator.
// It always uses acknowledged writes regardless of the manipulator's default
// write consistency override.
func CreateIndex(manipulator manipulate.Manipulator, identity elemental.Identity, indexes ...mongo.IndexModel) error {

	m, ok := manipulator.(*mongoManipulator)
	if !ok {
		panic("you can only pass a mongo manipulator to CreateIndex")
	}

	ctx, cancel := contextWithDefaultTimeout(context.Background(), m.operationTimeout)
	defer cancel()

	coll := m.makeAcknowledgedDatabase().Collection(identity.Name)
	indexView := coll.Indexes()

	for i, index := range indexes {
		preparedIndex, idxName, _, _, err := prepareMongoIndexModel(identity, i, index)
		if err != nil {
			return fmt.Errorf("unable to prepare index %d: %w", i, err)
		}
		if _, err := indexView.CreateOne(ctx, preparedIndex); err != nil {
			return fmt.Errorf("unable to ensure index '%s': %w", idxName, err)
		}
	}

	return nil
}

// EnsureIndex works like create index, but it will delete existing index
// if they changed before creating a new one.
// It always uses acknowledged writes regardless of the manipulator's default
// write consistency override.
func EnsureIndex(manipulator manipulate.Manipulator, identity elemental.Identity, indexes ...mongo.IndexModel) error {

	m, ok := manipulator.(*mongoManipulator)
	if !ok {
		panic("you can only pass a mongo manipulator to CreateIndex")
	}

	ctx, cancel := contextWithDefaultTimeout(context.Background(), m.operationTimeout)
	defer cancel()

	coll := m.makeAcknowledgedDatabase().Collection(identity.Name)
	indexView := coll.Indexes()

	for i, index := range indexes {
		preparedIndex, idxName, expireAfter, hasExpireAfter, err := prepareMongoIndexModel(identity, i, index)
		if err != nil {
			return fmt.Errorf("unable to prepare index %d: %w", i, err)
		}

		if _, err := indexView.CreateOne(ctx, preparedIndex); err != nil {
			if !isMongoIndexConflictError(err) {
				return fmt.Errorf("unable to ensure index '%s': %w", idxName, err)
			}

			// In case we are changing a TTL we use collMod, same rationale as legacy EnsureIndex.
			if hasExpireAfter && expireAfter > 0 {
				if err := coll.Database().RunCommand(ctx, odbson.D{
					{Key: "collMod", Value: coll.Name()},
					{Key: "index", Value: odbson.M{"name": idxName, "expireAfterSeconds": expireAfter}},
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
	}

	return nil
}

// DeleteIndex deletes multiple indexes for the collection.
// It always uses acknowledged writes regardless of the manipulator's default
// write consistency override.
func DeleteIndex(manipulator manipulate.Manipulator, identity elemental.Identity, indexes ...string) error {

	m, ok := manipulator.(*mongoManipulator)
	if !ok {
		panic("you can only pass a mongo manipulator to DeleteIndex")
	}

	ctx, cancel := contextWithDefaultTimeout(context.Background(), m.operationTimeout)
	defer cancel()

	coll := m.makeAcknowledgedDatabase().Collection(identity.Name)
	indexView := coll.Indexes()
	for _, index := range indexes {
		if err := indexView.DropOne(ctx, index); err != nil {
			return err
		}
	}

	return nil
}

// CreateCollection creates a collection using official mongo driver options.
// It always uses acknowledged writes regardless of the manipulator's default
// write consistency override.
func CreateCollection(
	manipulator manipulate.Manipulator,
	identity elemental.Identity,
	opts ...mongooptions.Lister[mongooptions.CreateCollectionOptions],
) error {

	m, ok := manipulator.(*mongoManipulator)
	if !ok {
		panic("you can only pass a mongo manipulator to CreateCollection")
	}

	ctx, cancel := contextWithDefaultTimeout(context.Background(), m.operationTimeout)
	defer cancel()

	if err := m.makeAcknowledgedDatabase().CreateCollection(ctx, identity.Name, opts...); err != nil {
		return fmt.Errorf("unable to create collection '%s': %w", identity.Name, err)
	}

	return nil
}

// GetDatabase returns a ready-to-use official-driver database object.
//
// The returned handle is derived from the manipmongo instance's current
// default read/write consistency configuration, including any overrides
// applied through SetConsistencyMode.
//
// Callers are responsible for passing explicit contexts with deadlines to raw
// driver operations performed through this handle. OptionSocketTimeout only
// applies to built-in manipmongo CRUD paths and is not injected into raw
// GetDatabase usage.
func GetDatabase(manipulator manipulate.Manipulator) (*mongo.Database, error) {

	m, ok := manipulator.(*mongoManipulator)
	if !ok {
		panic("you can only pass a mongo manipulator to GetDatabase")
	}

	return m.makeDatabase(manipulate.ReadConsistencyDefault, manipulate.WriteConsistencyDefault), nil
}

// Disconnect closes the underlying official-driver client used by the manipulator.
//
// If ctx is nil, Disconnect uses the manipulator's default operation timeout.
// This shuts down the entire client lifecycle for the manipulator, not just a
// temporary database handle returned by GetDatabase.
func Disconnect(manipulator manipulate.Manipulator, ctx context.Context) error {

	m, ok := manipulator.(*mongoManipulator)
	if !ok {
		panic("you can only pass a mongo manipulator to Disconnect")
	}

	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = contextWithDefaultTimeout(context.Background(), m.operationTimeout)
		defer cancel()
	}

	return mongoDisconnectFn(m.client, ctx)
}

// SetConsistencyMode sets the default read/write consistency mode of the mongo manipulator.
//
// This updates the defaults used by built-in CRUD operations and by GetDatabase.
// Passing manipulate.ReadConsistencyDefault or manipulate.WriteConsistencyDefault
// leaves the corresponding current default unchanged.
func SetConsistencyMode(
	manipulator manipulate.Manipulator,
	readConsistency manipulate.ReadConsistency,
	writeConsistency manipulate.WriteConsistency,
) error {

	m, ok := manipulator.(*mongoManipulator)
	if !ok {
		panic("you can only pass a mongo manipulator to SetConsistencyMode")
	}

	currentReadConsistency, currentWriteConsistency := m.defaultConsistency()
	if readConsistency == manipulate.ReadConsistencyDefault {
		readConsistency = currentReadConsistency
	}
	if writeConsistency == manipulate.WriteConsistencyDefault {
		writeConsistency = currentWriteConsistency
	}

	m.setDefaultConsistency(readConsistency, writeConsistency)
	return nil
}

// RunQuery runs a function that must run a MongoDB operation.
// It will retry in case of communication failure. This is an advanced helper
// that can be used when you get a database by calling GetDatabase().
//
// Plain context.Canceled and context.DeadlineExceeded errors returned by the
// callback are treated as non-retryable and mapped to ErrCannotExecuteQuery to
// avoid automatically replaying caller-owned write operations.
func RunQuery(mctx manipulate.Context, operationFunc func() (any, error), baseRetryInfo RetryInfo) (any, error) {

	var try int

	for {

		out, err := operationFunc()
		if err == nil {
			return out, nil
		}

		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return out, manipulate.ErrCannotExecuteQuery{Err: err}
		}

		err = HandleQueryErrorMongo(err)
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

// SetAttributeEncrypter switches the attribute encrypter of the given mongo manipulator.
// This is only useful in some rare cases like migration, and it is not goroutine-safe.
func SetAttributeEncrypter(manipulator manipulate.Manipulator, enc elemental.AttributeEncrypter) {

	m, ok := manipulator.(*mongoManipulator)
	if !ok {
		panic("you can only pass a mongo manipulator to SetAttributeEncrypter")
	}

	m.attributeEncrypter = enc
}

// GetAttributeEncrypter returns the attribute encrypter of the given mongo manipulator.
func GetAttributeEncrypter(manipulator manipulate.Manipulator) elemental.AttributeEncrypter {

	m, ok := manipulator.(*mongoManipulator)
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
	_, ok := manipulator.(*mongoManipulator)

	return ok
}

func prepareMongoIndexModel(identity elemental.Identity, indexPos int, model mongo.IndexModel) (mongo.IndexModel, string, int32, bool, error) {

	model.Options = cloneMongoIndexOptionsBuilder(model.Options)

	applied, err := applyMongoIndexOptions(model.Options)
	if err != nil {
		return mongo.IndexModel{}, "", 0, false, err
	}

	idxName := ""
	if applied.Name != nil {
		idxName = *applied.Name
	}
	if idxName == "" {
		idxName = "index_" + identity.Name + "_" + strconv.Itoa(indexPos)
		model.Options.SetName(idxName)
	}

	if applied.ExpireAfterSeconds != nil {
		return model, idxName, *applied.ExpireAfterSeconds, true, nil
	}

	return model, idxName, 0, false, nil
}

func cloneMongoIndexOptionsBuilder(builder *mongooptions.IndexOptionsBuilder) *mongooptions.IndexOptionsBuilder {
	cloned := mongooptions.Index()
	if builder == nil {
		return cloned
	}
	cloned.Opts = append(cloned.Opts, builder.List()...)
	return cloned
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
