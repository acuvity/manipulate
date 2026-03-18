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
	"maps"
	neturl "net/url"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go/log"
	"go.acuvity.ai/elemental"
	"go.acuvity.ai/manipulate"
	"go.acuvity.ai/manipulate/internal/backoff"
	"go.acuvity.ai/manipulate/internal/tracing"
	bson "go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

const defaultOperationTimeout = 60 * time.Second

// New returns a new manipulator backed by the official mongo driver.
func New(url string, db string, options ...Option) (manipulate.TransactionalManipulator, error) {
	return newMongo(url, db, options...)
}

type countRes struct {
	Count int `bson:"_count"`
}

type mongoOperationContextError struct {
	Err error
}

func (e *mongoOperationContextError) Error() string {
	if e == nil || e.Err == nil {
		return ""
	}
	return e.Err.Error()
}

func (e *mongoOperationContextError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func countFromResults(res []*countRes) (int, error) {
	switch len(res) {
	case 0:
		return 0, nil
	case 1:
		return res[0].Count, nil
	default:
		return 0, manipulate.ErrCannotExecuteQuery{Err: fmt.Errorf("count: invalid count result len: %d. must be 1", len(res))}
	}
}

// mongoManipulator implements manipulator operations using the official
// mongo driver.
type mongoManipulator struct {
	client                  *mongo.Client
	dbName                  string
	sharder                 Sharder
	defaultRetryFunc        manipulate.RetryFunc
	forcedReadFilter        bson.D
	attributeEncrypter      elemental.AttributeEncrypter
	explain                 map[elemental.Identity]map[elemental.Operation]struct{}
	attributeSpecifiers     map[elemental.Identity]elemental.AttributeSpecifiable
	operationTimeout        time.Duration
	defaultReadConsistency  manipulate.ReadConsistency
	defaultWriteConsistency manipulate.WriteConsistency
	consistencyMu           sync.RWMutex
}

var (
	mongoConnectFn = func(opts ...*options.ClientOptions) (*mongo.Client, error) {
		return mongo.Connect(opts...)
	}
	mongoPingFn = func(client *mongo.Client, ctx context.Context) error {
		return client.Ping(ctx, nil)
	}
	mongoDisconnectFn = func(client *mongo.Client, ctx context.Context) error {
		return client.Disconnect(ctx)
	}
)

func redactMongoURI(raw string) string {
	parsed, err := neturl.Parse(raw)
	if err != nil || parsed == nil || parsed.User == nil {
		return raw
	}

	username := parsed.User.Username()
	if username == "" {
		username = "redacted"
	}

	if _, hasPassword := parsed.User.Password(); hasPassword {
		parsed.User = neturl.UserPassword(username, "REDACTED")
		return parsed.String()
	}

	parsed.User = neturl.User(username)
	return parsed.String()
}

func newMongo(url string, db string, opts ...Option) (manipulate.TransactionalManipulator, error) {

	cfg := newConfig()
	for _, o := range opts {
		o(cfg)
	}

	if cfg.poolLimit < 0 {
		return nil, fmt.Errorf("invalid connection pool limit %d: must be greater than or equal to 0", cfg.poolLimit)
	}

	var (
		validatedForcedReadFilter bson.D
		err                       error
	)
	if cfg.forcedReadFilter != nil {
		if len(cfg.forcedReadFilter) == 0 {
			validatedForcedReadFilter = bson.D{}
		} else {
			validatedForcedReadFilter, err = cloneBSONValue[bson.D](cfg.forcedReadFilter)
			if err != nil {
				return nil, fmt.Errorf("unable to process forced read filter: %w", err)
			}
		}
	}

	sharder := cfg.sharder

	mongoPoolLimit := cfg.poolLimit
	if mongoPoolLimit == 0 {
		mongoPoolLimit = newConfig().poolLimit
	}

	clientOpts := options.Client().
		ApplyURI(url).
		SetMaxPoolSize(uint64(mongoPoolLimit)).
		SetConnectTimeout(cfg.connectTimeout)

	if cfg.clientTimeout > 0 {
		clientOpts.SetTimeout(cfg.clientTimeout)
	}

	if cfg.username != "" || cfg.password != "" || cfg.authsource != "" {
		clientOpts.SetAuth(options.Credential{
			Username:   cfg.username,
			Password:   cfg.password,
			AuthSource: cfg.authsource,
		})
	}

	if cfg.tlsConfig != nil {
		clientOpts.SetTLSConfig(cfg.tlsConfig)
	}

	if rp := convertReadPreferenceMongo(cfg.readConsistency); rp != nil {
		clientOpts.SetReadPreference(rp)
	}
	if wc := convertWriteConcernMongo(cfg.writeConsistency); wc != nil {
		clientOpts.SetWriteConcern(wc)
	}

	client, err := mongoConnectFn(clientOpts)
	if err != nil {
		return nil, fmt.Errorf("cannot connect to mongo url '%s': %w", redactMongoURI(url), err)
	}

	ctx, cancel := contextWithOptionalTimeout(context.Background(), cfg.connectTimeout)
	defer cancel()
	if err := mongoPingFn(client, ctx); err != nil {
		disconnectCtx, disconnectCancel := contextWithOptionalTimeout(context.Background(), cfg.connectTimeout)
		defer disconnectCancel()
		_ = mongoDisconnectFn(client, disconnectCtx)
		return nil, fmt.Errorf("cannot ping mongo url '%s': %w", redactMongoURI(url), err)
	}

	return &mongoManipulator{
		client:                  client,
		dbName:                  db,
		sharder:                 sharder,
		defaultRetryFunc:        cfg.defaultRetryFunc,
		forcedReadFilter:        validatedForcedReadFilter,
		attributeEncrypter:      cfg.attributeEncrypter,
		explain:                 cfg.explain,
		attributeSpecifiers:     cfg.attributeSpecifiers,
		operationTimeout:        cfg.operationTimeout,
		defaultReadConsistency:  cfg.readConsistency,
		defaultWriteConsistency: cfg.writeConsistency,
	}, nil
}

func (m *mongoManipulator) RetrieveMany(mctx manipulate.Context, dest elemental.Identifiables) error {
	if mctx == nil {
		ctx, cancel := m.defaultContext()
		defer cancel()
		mctx = manipulate.NewContext(ctx)
	}

	sp := tracing.StartTrace(mctx, fmt.Sprintf("manipmongo.retrieve_many.mongo.%s", dest.Identity().Category))
	defer sp.Finish()

	var attrSpec elemental.AttributeSpecifiable
	if m.attributeSpecifiers != nil {
		attrSpec = m.attributeSpecifiers[dest.Identity()]
	}

	var order []string
	if o := mctx.Order(); len(o) > 0 {
		order = applyOrdering(o, attrSpec)
	} else if orderer, ok := dest.(elemental.DefaultOrderer); ok {
		order = applyOrdering(orderer.DefaultOrder(), attrSpec)
	}

	coll := m.makeCollection(dest.Identity(), mctx.ReadConsistency(), mctx.WriteConsistency())

	shardingFilter, err := m.makeShardingManyFilter(mctx, dest.Identity())
	if err != nil {
		return spanErr(sp, err)
	}

	pipeline, err := makePipeline(
		attrSpec,
		m.makePreviousRetriever(mctx.Context(), coll),
		shardingFilter,
		makeNamespaceFilter(mctx),
		m.forcedReadFilter,
		makeUserFilter(mctx, attrSpec),
		order,
		mctx.After(),
		mctx.Limit(),
		mctx.Fields(),
	)
	if err != nil {
		return spanErr(sp, err)
	}

	sp.LogFields(log.Object("pipeline", pipeline))

	aggregateExplainCommand := bson.D{
		{Key: "aggregate", Value: coll.Name()},
		{Key: "pipeline", Value: pipeline},
		{Key: "cursor", Value: bson.D{}},
	}

	if _, err := runQueryMongo(
		mctx,
		func() (any, error) {
			if shouldExplain(dest.Identity(), elemental.OperationRetrieveMany, m.explain) {
				if err := explainMongo(mctx.Context(), m.operationTimeout, coll.Database(), aggregateExplainCommand, elemental.OperationRetrieveMany, dest.Identity(), pipeline); err != nil {
					return nil, manipulate.ErrCannotBuildQuery{Err: fmt.Errorf("retrievemany: unable to explain: %w", err)}
				}
			}

			queryCtx, cancel, err := m.queryContext(mctx.Context())
			if err != nil {
				return nil, err
			}
			defer cancel()

			cur, err := coll.Aggregate(queryCtx, pipeline)
			if err != nil {
				return nil, wrapMongoOperationContextError(mctx.Context(), queryCtx, err)
			}
			defer func() { _ = cur.Close(queryCtx) }()
			return nil, wrapMongoOperationContextError(mctx.Context(), queryCtx, cur.All(queryCtx, dest))
		},
		RetryInfo{
			Operation:        elemental.OperationRetrieveMany,
			Identity:         dest.Identity(),
			defaultRetryFunc: m.defaultRetryFunc,
		},
	); err != nil {
		return spanErr(sp, err)
	}

	var lastID string
	lst := dest.List()
	for _, o := range lst {
		if a, ok := o.(elemental.AttributeSpecifiable); ok {
			elemental.ResetDefaultForZeroValues(a)
		}
		if m.attributeEncrypter != nil {
			if a, ok := o.(elemental.AttributeEncryptable); ok {
				if err := a.DecryptAttributes(m.attributeEncrypter); err != nil {
					return spanErr(sp, manipulate.ErrCannotBuildQuery{Err: fmt.Errorf("retrievemany: unable to decrypt attributes: %w", err)})
				}
			}
		}
		lastID = o.Identifier()
	}

	if lastID != "" && (mctx.After() != "" || mctx.Limit() > 0) && len(lst) == mctx.Limit() {
		if lastID != mctx.After() {
			mctx.SetNext(lastID)
		}
	}

	return nil
}

func (m *mongoManipulator) Retrieve(mctx manipulate.Context, object elemental.Identifiable) error {
	if mctx == nil {
		ctx, cancel := m.defaultContext()
		defer cancel()
		mctx = manipulate.NewContext(ctx)
	}

	sp := tracing.StartTrace(mctx, fmt.Sprintf("manipmongo.retrieve.mongo.object.%s", object.Identity().Name))
	sp.LogFields(log.String("object_id", object.Identifier()))
	defer sp.Finish()

	var attrSpec elemental.AttributeSpecifiable
	if m.attributeSpecifiers != nil {
		attrSpec = m.attributeSpecifiers[object.Identity()]
	}

	filter := bson.D{}
	if f := mctx.Filter(); f != nil {
		filter = makeUserFilter(mctx, attrSpec)
	}

	filter = append(filter, identifierFilterElement(object.Identifier()))

	var ands []bson.D
	if sq, err := m.makeShardingOneFilter(mctx, object); err != nil {
		return spanErr(sp, err)
	} else if sq != nil {
		ands = append(ands, sq)
	}
	if mctx.Namespace() != "" {
		ands = append(ands, makeNamespaceFilter(mctx))
	}
	if m.forcedReadFilter != nil {
		ands = append(ands, m.forcedReadFilter)
	}
	filter = composeAndFilter(filter, ands...)
	sp.LogFields(log.Object("filter", filter))

	coll := m.makeCollection(object.Identity(), mctx.ReadConsistency(), mctx.WriteConsistency())
	findOpts := options.FindOne()
	projection := makeFieldsSelector(mctx.Fields(), attrSpec)
	if projection != nil {
		findOpts.SetProjection(projection)
	}
	findExplainCommand := bson.D{
		{Key: "find", Value: coll.Name()},
		{Key: "filter", Value: filter},
		{Key: "limit", Value: 1},
	}
	if projection != nil {
		findExplainCommand = append(findExplainCommand, bson.E{Key: "projection", Value: projection})
	}
	if _, err := runQueryMongo(
		mctx,
		func() (any, error) {
			if shouldExplain(object.Identity(), elemental.OperationRetrieve, m.explain) {
				if err := explainMongo(mctx.Context(), m.operationTimeout, coll.Database(), findExplainCommand, elemental.OperationRetrieve, object.Identity(), filter); err != nil {
					return nil, manipulate.ErrCannotBuildQuery{Err: fmt.Errorf("retrieve: unable to explain: %w", err)}
				}
			}
			queryCtx, cancel, err := m.queryContext(mctx.Context())
			if err != nil {
				return nil, err
			}
			defer cancel()
			return nil, wrapMongoOperationContextError(mctx.Context(), queryCtx, coll.FindOne(queryCtx, filter, findOpts).Decode(object))
		},
		RetryInfo{
			Operation:        elemental.OperationRetrieve,
			Identity:         object.Identity(),
			defaultRetryFunc: m.defaultRetryFunc,
		},
	); err != nil {
		return spanErr(sp, err)
	}

	if a, ok := object.(elemental.AttributeSpecifiable); ok {
		elemental.ResetDefaultForZeroValues(a)
	}
	if m.attributeEncrypter != nil {
		if a, ok := object.(elemental.AttributeEncryptable); ok {
			if err := a.DecryptAttributes(m.attributeEncrypter); err != nil {
				return spanErr(sp, manipulate.ErrCannotBuildQuery{Err: fmt.Errorf("retrieve: unable to decrypt attributes: %w", err)})
			}
		}
	}
	return nil
}

func (m *mongoManipulator) Create(mctx manipulate.Context, object elemental.Identifiable) error {
	if mctx == nil {
		ctx, cancel := m.defaultContext()
		defer cancel()
		mctx = manipulate.NewContext(ctx)
	}

	sp := tracing.StartTrace(mctx, fmt.Sprintf("manipmongo.create.mongo.object.%s", object.Identity().Name))
	sp.LogFields(log.String("object_id", object.Identifier()))
	defer sp.Finish()

	oid := bson.NewObjectID()
	object.SetIdentifier(oid.Hex())

	if f := mctx.Finalizer(); f != nil {
		if err := f(object); err != nil {
			return spanErr(sp, err)
		}
	}

	if m.sharder != nil {
		if err := m.sharder.Shard(m, mctx, object); err != nil {
			return spanErr(sp, manipulate.ErrCannotBuildQuery{Err: fmt.Errorf("unable to execute sharder.Shard: %w", err)})
		}
	}

	var encryptable elemental.AttributeEncryptable
	if m.attributeEncrypter != nil {
		if a, ok := object.(elemental.AttributeEncryptable); ok {
			encryptable = a
			if err := a.EncryptAttributes(m.attributeEncrypter); err != nil {
				return spanErr(sp, manipulate.ErrCannotBuildQuery{Err: fmt.Errorf("create: unable to encrypt attributes: %w", err)})
			}
		}
	}

	if a, ok := object.(elemental.AttributeSpecifiable); ok {
		elemental.ResetDefaultForZeroValues(a)
	}

	coll := m.makeCollection(object.Identity(), mctx.ReadConsistency(), mctx.WriteConsistency())

	if operations, upsert := mctx.(opaquer).Opaque()[opaqueKeyUpsert]; upsert {
		object.SetIdentifier("")

		ops, err := convertUpsertOperationsToMongo(operations)
		if err != nil {
			return spanErr(sp, manipulate.ErrCannotBuildQuery{Err: err})
		}

		baseOps := bson.M{
			"$set":         object,
			"$setOnInsert": bson.M{"_id": oid},
		}
		if len(ops) > 0 {
			if soi, ok := ops["$setOnInsert"]; ok {
				if err := mergeSetOnInsertOperationsMongo(baseOps["$setOnInsert"].(bson.M), soi); err != nil {
					return spanErr(sp, manipulate.ErrCannotBuildQuery{Err: err})
				}
			}
			for k, v := range ops {
				if k == "$setOnInsert" {
					continue
				}
				baseOps[k] = v
			}
		}

		var attrSpec elemental.AttributeSpecifiable
		if m.attributeSpecifiers != nil {
			attrSpec = m.attributeSpecifiers[object.Identity()]
		}

		filter := bson.D{}
		if f := mctx.Filter(); f != nil {
			filter = makeUserFilter(mctx, attrSpec)
		}
		var ands []bson.D
		if sq, err := m.makeShardingOneFilter(mctx, object); err != nil {
			return spanErr(sp, err)
		} else if sq != nil {
			ands = append(ands, sq)
		}
		if mctx.Namespace() != "" {
			ands = append(ands, makeNamespaceFilter(mctx))
		}
		if m.forcedReadFilter != nil {
			ands = append(ands, m.forcedReadFilter)
		}
		filter = composeAndFilter(filter, ands...)

		info, err := runQueryMongo(
			mctx,
			func() (any, error) {
				queryCtx, cancel, err := m.queryContext(mctx.Context())
				if err != nil {
					return nil, err
				}
				defer cancel()
				r, err := coll.UpdateOne(queryCtx, filter, baseOps, options.UpdateOne().SetUpsert(true))
				if err != nil {
					return nil, wrapMongoOperationContextError(mctx.Context(), queryCtx, err)
				}
				return r, nil
			},
			RetryInfo{
				Operation:        elemental.OperationCreate,
				Identity:         object.Identity(),
				defaultRetryFunc: m.defaultRetryFunc,
			},
		)
		if err != nil {
			return spanErr(sp, err)
		}

		if r, ok := info.(*mongo.UpdateResult); ok && r.UpsertedID != nil {
			switch id := r.UpsertedID.(type) {
			case bson.ObjectID:
				object.SetIdentifier(id.Hex())
			case string:
				object.SetIdentifier(id)
			}
		}
	} else {
		if _, err := runQueryMongo(
			mctx,
			func() (any, error) {
				queryCtx, cancel, err := m.queryContext(mctx.Context())
				if err != nil {
					return nil, err
				}
				defer cancel()
				r, err := coll.InsertOne(queryCtx, object)
				if err != nil {
					return nil, wrapMongoOperationContextError(mctx.Context(), queryCtx, err)
				}
				return r, nil
			},
			RetryInfo{
				Operation:        elemental.OperationCreate,
				Identity:         object.Identity(),
				defaultRetryFunc: m.defaultRetryFunc,
			},
		); err != nil {
			return spanErr(sp, err)
		}
	}

	if encryptable != nil {
		if err := encryptable.DecryptAttributes(m.attributeEncrypter); err != nil {
			return spanErr(sp, manipulate.ErrCannotBuildQuery{Err: fmt.Errorf("create: unable to decrypt attributes: %w", err)})
		}
	}
	if m.sharder != nil {
		if err := m.sharder.OnShardedWrite(m, mctx, elemental.OperationCreate, object); err != nil {
			return spanErr(sp, manipulate.ErrCannotBuildQuery{Err: fmt.Errorf("unable to execute sharder.OnShardedWrite on create: %w", err)})
		}
	}

	return nil
}

func (m *mongoManipulator) Update(mctx manipulate.Context, object elemental.Identifiable) error {
	if mctx == nil {
		ctx, cancel := m.defaultContext()
		defer cancel()
		mctx = manipulate.NewContext(ctx)
	}

	sp := tracing.StartTrace(mctx, fmt.Sprintf("manipmongo.update.mongo.object.%s", object.Identity().Name))
	sp.LogFields(log.String("object_id", object.Identifier()))
	defer sp.Finish()

	var encryptable elemental.AttributeEncryptable
	if m.attributeEncrypter != nil {
		if a, ok := object.(elemental.AttributeEncryptable); ok {
			encryptable = a
			if err := a.EncryptAttributes(m.attributeEncrypter); err != nil {
				return spanErr(sp, manipulate.ErrCannotBuildQuery{Err: fmt.Errorf("update: unable to encrypt attributes: %w", err)})
			}
		}
	}

	if a, ok := object.(elemental.AttributeSpecifiable); ok {
		elemental.ResetDefaultForZeroValues(a)
	}

	filter := bson.D{identifierFilterElement(object.Identifier())}
	var ands []bson.D
	if sq, err := m.makeShardingOneFilter(mctx, object); err != nil {
		return spanErr(sp, err)
	} else if sq != nil {
		ands = append(ands, sq)
	}
	if mctx.Namespace() != "" {
		ands = append(ands, makeNamespaceFilter(mctx))
	}
	if m.forcedReadFilter != nil {
		ands = append(ands, m.forcedReadFilter)
	}
	filter = composeAndFilter(filter, ands...)

	coll := m.makeCollection(object.Identity(), mctx.ReadConsistency(), mctx.WriteConsistency())
	if _, err := runQueryMongo(
		mctx,
		func() (any, error) {
			queryCtx, cancel, err := m.queryContext(mctx.Context())
			if err != nil {
				return nil, err
			}
			defer cancel()
			r, err := coll.UpdateOne(queryCtx, filter, bson.M{"$set": object})
			if err != nil {
				return nil, wrapMongoOperationContextError(mctx.Context(), queryCtx, err)
			}
			if r.Acknowledged && r.MatchedCount == 0 {
				return nil, mongo.ErrNoDocuments
			}
			return r, nil
		},
		RetryInfo{
			Operation:        elemental.OperationUpdate,
			Identity:         object.Identity(),
			defaultRetryFunc: m.defaultRetryFunc,
		},
	); err != nil {
		return spanErr(sp, err)
	}

	if encryptable != nil {
		if err := encryptable.DecryptAttributes(m.attributeEncrypter); err != nil {
			return spanErr(sp, manipulate.ErrCannotBuildQuery{Err: fmt.Errorf("update: unable to decrypt attributes: %w", err)})
		}
	}

	return nil
}

func (m *mongoManipulator) Delete(mctx manipulate.Context, object elemental.Identifiable) error {
	if mctx == nil {
		ctx, cancel := m.defaultContext()
		defer cancel()
		mctx = manipulate.NewContext(ctx)
	}

	sp := tracing.StartTrace(mctx, fmt.Sprintf("manipmongo.delete.mongo.object.%s", object.Identity().Name))
	sp.LogFields(log.String("object_id", object.Identifier()))
	defer sp.Finish()

	var attrSpec elemental.AttributeSpecifiable
	if m.attributeSpecifiers != nil {
		attrSpec = m.attributeSpecifiers[object.Identity()]
	}

	filter := bson.D{}
	if f := mctx.Filter(); f != nil {
		filter = makeUserFilter(mctx, attrSpec)
	}
	filter = append(filter, identifierFilterElement(object.Identifier()))
	var ands []bson.D
	if sq, err := m.makeShardingOneFilter(mctx, object); err != nil {
		return spanErr(sp, err)
	} else if sq != nil {
		ands = append(ands, sq)
	}
	if mctx.Namespace() != "" {
		ands = append(ands, makeNamespaceFilter(mctx))
	}
	if m.forcedReadFilter != nil {
		ands = append(ands, m.forcedReadFilter)
	}
	filter = composeAndFilter(filter, ands...)
	sp.LogFields(log.Object("filter", filter))

	coll := m.makeCollection(object.Identity(), mctx.ReadConsistency(), mctx.WriteConsistency())
	if _, err := runQueryMongo(
		mctx,
		func() (any, error) {
			queryCtx, cancel, err := m.queryContext(mctx.Context())
			if err != nil {
				return nil, err
			}
			defer cancel()
			r, err := coll.DeleteOne(queryCtx, filter)
			if err != nil {
				return nil, wrapMongoOperationContextError(mctx.Context(), queryCtx, err)
			}
			if r.Acknowledged && r.DeletedCount == 0 {
				return nil, mongo.ErrNoDocuments
			}
			return r, nil
		},
		RetryInfo{
			Operation:        elemental.OperationDelete,
			Identity:         object.Identity(),
			defaultRetryFunc: m.defaultRetryFunc,
		},
	); err != nil {
		return spanErr(sp, err)
	}

	if m.sharder != nil {
		if err := m.sharder.OnShardedWrite(m, mctx, elemental.OperationDelete, object); err != nil {
			return spanErr(sp, manipulate.ErrCannotBuildQuery{Err: fmt.Errorf("unable to execute sharder.OnShardedWrite for delete: %w", err)})
		}
	}
	if a, ok := object.(elemental.AttributeSpecifiable); ok {
		elemental.ResetDefaultForZeroValues(a)
	}

	return nil
}

func (m *mongoManipulator) DeleteMany(mctx manipulate.Context, identity elemental.Identity) error {
	if mctx == nil {
		ctx, cancel := m.defaultContext()
		defer cancel()
		mctx = manipulate.NewContext(ctx)
	}

	sp := tracing.StartTrace(mctx, fmt.Sprintf("manipmongo.delete_many.mongo.%s", identity.Name))
	defer sp.Finish()

	var attrSpec elemental.AttributeSpecifiable
	if m.attributeSpecifiers != nil {
		attrSpec = m.attributeSpecifiers[identity]
	}

	filter := bson.D{}
	if f := mctx.Filter(); f != nil {
		filter = makeUserFilter(mctx, attrSpec)
	}
	var ands []bson.D
	if sq, err := m.makeShardingManyFilter(mctx, identity); err != nil {
		return spanErr(sp, err)
	} else if sq != nil {
		ands = append(ands, sq)
	}
	if mctx.Namespace() != "" {
		ands = append(ands, makeNamespaceFilter(mctx))
	}
	if m.forcedReadFilter != nil {
		ands = append(ands, m.forcedReadFilter)
	}
	filter = composeAndFilter(filter, ands...)
	sp.LogFields(log.Object("filter", filter))

	coll := m.makeCollection(identity, mctx.ReadConsistency(), mctx.WriteConsistency())
	if _, err := runQueryMongo(
		mctx,
		func() (any, error) {
			queryCtx, cancel, err := m.queryContext(mctx.Context())
			if err != nil {
				return nil, err
			}
			defer cancel()
			r, err := coll.DeleteMany(queryCtx, filter)
			if err != nil {
				return nil, wrapMongoOperationContextError(mctx.Context(), queryCtx, err)
			}
			return r, nil
		},
		RetryInfo{
			Operation:        elemental.OperationDelete,
			Identity:         identity,
			defaultRetryFunc: m.defaultRetryFunc,
		},
	); err != nil {
		return spanErr(sp, err)
	}

	return nil
}

func (m *mongoManipulator) Count(mctx manipulate.Context, identity elemental.Identity) (int, error) {
	if mctx == nil {
		ctx, cancel := m.defaultContext()
		defer cancel()
		mctx = manipulate.NewContext(ctx)
	}

	sp := tracing.StartTrace(mctx, fmt.Sprintf("manipmongo.count.mongo.%s", identity.Category))
	defer sp.Finish()

	var attrSpec elemental.AttributeSpecifiable
	if m.attributeSpecifiers != nil {
		attrSpec = m.attributeSpecifiers[identity]
	}

	coll := m.makeCollection(identity, mctx.ReadConsistency(), mctx.WriteConsistency())

	shardingFilter, err := m.makeShardingManyFilter(mctx, identity)
	if err != nil {
		return 0, spanErr(sp, err)
	}

	pipeline, err := makePipeline(
		attrSpec,
		m.makePreviousRetriever(mctx.Context(), coll),
		shardingFilter,
		makeNamespaceFilter(mctx),
		m.forcedReadFilter,
		makeUserFilter(mctx, attrSpec),
		nil,
		mctx.After(),
		mctx.Limit(),
		append(mctx.Fields(), "_id"),
	)
	if err != nil {
		return 0, spanErr(sp, err)
	}

	pipeline = append(pipeline, bson.D{{Key: "$count", Value: "_count"}})
	sp.LogFields(log.Object("pipeline", pipeline))

	aggOpts := options.Aggregate()
	aggregateExplainCommand := bson.D{
		{Key: "aggregate", Value: coll.Name()},
		{Key: "pipeline", Value: pipeline},
		{Key: "cursor", Value: bson.D{}},
	}

	res := []*countRes{}
	if _, err := runQueryMongo(
		mctx,
		func() (any, error) {
			if shouldExplain(identity, elemental.OperationInfo, m.explain) {
				if err := explainMongo(mctx.Context(), m.operationTimeout, coll.Database(), aggregateExplainCommand, elemental.OperationInfo, identity, pipeline); err != nil {
					return nil, manipulate.ErrCannotBuildQuery{Err: fmt.Errorf("count: unable to explain: %w", err)}
				}
			}
			queryCtx, cancel, err := m.queryContext(mctx.Context())
			if err != nil {
				return nil, err
			}
			defer cancel()
			cur, err := coll.Aggregate(queryCtx, pipeline, aggOpts)
			if err != nil {
				return nil, wrapMongoOperationContextError(mctx.Context(), queryCtx, err)
			}
			defer func() { _ = cur.Close(queryCtx) }()
			return nil, wrapMongoOperationContextError(mctx.Context(), queryCtx, cur.All(queryCtx, &res))
		},
		RetryInfo{
			Operation:        elemental.OperationInfo,
			Identity:         identity,
			defaultRetryFunc: m.defaultRetryFunc,
		},
	); err != nil {
		return 0, spanErr(sp, err)
	}

	return countFromResults(res)
}

// Commit currently keeps legacy parity and is a no-op for the official-driver manipulator.
// Transactions are not implemented in this migration step.
func (m *mongoManipulator) Commit(id manipulate.TransactionID) error { return nil }

// Abort currently keeps legacy parity and is a no-op for the official-driver manipulator.
// Transactions are not implemented in this migration step.
func (m *mongoManipulator) Abort(id manipulate.TransactionID) bool { return true }

func (m *mongoManipulator) Ping(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return m.client.Ping(ctx, nil)
}

func contextWithOptionalTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, timeout)
}

func wrapMongoOperationContextError(parentCtx context.Context, queryCtx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)) && queryCtx != nil && queryCtx.Err() != nil && parentCtx != nil && parentCtx.Err() == nil {
		return &mongoOperationContextError{Err: queryCtx.Err()}
	}
	return err
}

func (m *mongoManipulator) defaultContext() (context.Context, context.CancelFunc) {
	return contextWithOptionalTimeout(context.Background(), m.operationTimeout)
}

func (m *mongoManipulator) queryContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	return mongoOperationContext(ctx, m.operationTimeout)
}

func (m *mongoManipulator) makeDatabase(readConsistency manipulate.ReadConsistency, writeConsistency manipulate.WriteConsistency) *mongo.Database {
	defaultReadConsistency, defaultWriteConsistency := m.defaultConsistency()
	if readConsistency == manipulate.ReadConsistencyDefault {
		readConsistency = defaultReadConsistency
	}
	if writeConsistency == manipulate.WriteConsistencyDefault {
		writeConsistency = defaultWriteConsistency
	}

	opts := options.Database()
	if rp := convertReadPreferenceMongo(readConsistency); rp != nil {
		opts.SetReadPreference(rp)
	}
	if wc := convertWriteConcernMongo(writeConsistency); wc != nil {
		opts.SetWriteConcern(wc)
	}
	return m.client.Database(m.dbName, opts)
}

func (m *mongoManipulator) makeAcknowledgedDatabase() *mongo.Database {
	defaultReadConsistency, _ := m.defaultConsistency()

	opts := options.Database().SetWriteConcern(writeconcern.W1())
	if rp := convertReadPreferenceMongo(defaultReadConsistency); rp != nil {
		opts.SetReadPreference(rp)
	}
	return m.client.Database(m.dbName, opts)
}

func (m *mongoManipulator) makeCollection(identity elemental.Identity, readConsistency manipulate.ReadConsistency, writeConsistency manipulate.WriteConsistency) *mongo.Collection {
	return m.makeDatabase(readConsistency, writeConsistency).Collection(identity.Name)
}

func (m *mongoManipulator) makeShardingOneFilter(mctx manipulate.Context, object elemental.Identifiable) (bson.D, error) {
	if m.sharder == nil {
		return nil, nil
	}
	filter, err := m.sharder.FilterOne(m, mctx, object)
	if err != nil {
		return nil, manipulate.ErrCannotBuildQuery{Err: fmt.Errorf("cannot compute sharding filter: %w", err)}
	}
	return filter, nil
}

func (m *mongoManipulator) makeShardingManyFilter(mctx manipulate.Context, identity elemental.Identity) (bson.D, error) {
	if m.sharder == nil {
		return nil, nil
	}
	filter, err := m.sharder.FilterMany(m, mctx, identity)
	if err != nil {
		return nil, manipulate.ErrCannotBuildQuery{Err: fmt.Errorf("cannot compute sharding filter: %w", err)}
	}
	return filter, nil
}

func identifierFilterElement(identifier string) bson.E {
	if oid, err := bson.ObjectIDFromHex(identifier); err == nil {
		return bson.E{Key: "_id", Value: oid}
	}
	return bson.E{Key: "_id", Value: identifier}
}

func composeAndFilter(filter bson.D, andFilters ...bson.D) bson.D {
	if len(andFilters) == 0 {
		return filter
	}

	clauses := make([]bson.D, 0, len(andFilters)+1)
	for _, clause := range andFilters {
		if len(clause) == 0 {
			continue
		}
		clauses = append(clauses, clause)
	}
	clauses = append(clauses, filter)

	return bson.D{{Key: "$and", Value: clauses}}
}

func (m *mongoManipulator) makePreviousRetriever(ctx context.Context, coll *mongo.Collection) func(id bson.ObjectID) (bson.M, error) {
	return func(id bson.ObjectID) (bson.M, error) {
		queryCtx, cancel, err := m.queryContext(ctx)
		if err != nil {
			return nil, err
		}
		defer cancel()

		doc := bson.M{}
		if err := coll.FindOne(queryCtx, bson.D{{Key: "_id", Value: id}}).Decode(&doc); err != nil {
			return nil, wrapMongoOperationContextError(ctx, queryCtx, err)
		}
		return doc, nil
	}
}

func convertReadPreferenceMongo(c manipulate.ReadConsistency) *readpref.ReadPref {
	switch c {
	case manipulate.ReadConsistencyEventual:
		// mgo.Eventual is effectively Nearest, but without session stickiness
		// across sequential reads. The official driver does not expose that
		// stickiness behavior as a read preference, so Nearest is the closest
		// semantic match.
		return readpref.Nearest()
	case manipulate.ReadConsistencyMonotonic:
		// Monotonic has no exact equivalent in mongo-driver read preferences.
		// PrimaryPreferred is the safest approximation for read-after-write behavior.
		return readpref.PrimaryPreferred()
	case manipulate.ReadConsistencyNearest:
		return readpref.Nearest()
	case manipulate.ReadConsistencyStrong:
		return readpref.Primary()
	case manipulate.ReadConsistencyWeakest:
		return readpref.SecondaryPreferred()
	default:
		return nil
	}
}

func convertWriteConcernMongo(c manipulate.WriteConsistency) *writeconcern.WriteConcern {
	switch c {
	case manipulate.WriteConsistencyNone:
		return writeconcern.Unacknowledged()
	case manipulate.WriteConsistencyStrong:
		return writeconcern.Majority()
	case manipulate.WriteConsistencyStrongest:
		journaled := writeconcern.Journaled()
		journaled.W = writeconcern.WCMajority
		return journaled
	default:
		return writeconcern.W1()
	}
}

func maxTimeFromContext(ctx context.Context, defaultTimeout time.Duration) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, manipulate.ErrCannotBuildQuery{Err: err}
	}

	d, ok := ctx.Deadline()
	if !ok {
		return defaultTimeout, nil
	}
	mx := time.Until(d)
	return mx, nil
}

func mongoOperationContext(ctx context.Context, defaultTimeout time.Duration) (context.Context, context.CancelFunc, error) {
	maxTime, err := maxTimeFromContext(ctx, defaultTimeout)
	if err != nil {
		return nil, nil, err
	}
	if maxTime <= 0 {
		queryCtx, cancel := context.WithCancel(ctx)
		return queryCtx, cancel, nil
	}
	queryCtx, cancel := context.WithTimeout(ctx, maxTime)
	return queryCtx, cancel, nil
}

func (m *mongoManipulator) setDefaultConsistency(readConsistency manipulate.ReadConsistency, writeConsistency manipulate.WriteConsistency) {
	m.consistencyMu.Lock()
	defer m.consistencyMu.Unlock()
	m.defaultReadConsistency = readConsistency
	m.defaultWriteConsistency = writeConsistency
}

func (m *mongoManipulator) defaultConsistency() (manipulate.ReadConsistency, manipulate.WriteConsistency) {
	m.consistencyMu.RLock()
	defer m.consistencyMu.RUnlock()
	return m.defaultReadConsistency, m.defaultWriteConsistency
}

func runQueryMongo(mctx manipulate.Context, operationFunc func() (any, error), baseRetryInfo RetryInfo) (any, error) {
	var try int
	for {
		out, err := operationFunc()
		if err == nil {
			return out, nil
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

func HandleQueryErrorMongo(err error) error {
	return HandleQueryError(err)
}

func convertUpsertOperationsToMongo(operations any) (bson.M, error) {
	switch typed := operations.(type) {
	case bson.M:
		if typed == nil {
			return bson.M{}, nil
		}
		return typed, nil
	default:
		return nil, fmt.Errorf("upsert operations must be bson.M")
	}
}

func mergeSetOnInsertOperationsMongo(dst bson.M, soi any) error {
	converted, err := convertSetOnInsertToMongoMap(soi)
	if err != nil {
		return err
	}
	maps.Copy(dst, converted)
	return nil
}

func convertSetOnInsertToMongoMap(soi any) (bson.M, error) {
	switch typed := soi.(type) {
	case bson.M:
		if typed == nil {
			return bson.M{}, nil
		}
		return typed, nil
	default:
		return nil, fmt.Errorf("$setOnInsert operations must be bson.M")
	}
}
