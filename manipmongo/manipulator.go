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
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/opentracing/opentracing-go/log"
	"go.acuvity.ai/elemental"
	"go.acuvity.ai/manipulate"
	"go.acuvity.ai/manipulate/internal/objectid"
	"go.acuvity.ai/manipulate/internal/tracing"
)

const defaultGlobalContextTimeout = 60 * time.Second

// MongoStore represents a MongoDB session.
type mongoManipulator struct {
	rootSession         *mgo.Session
	dbName              string
	sharder             Sharder
	defaultRetryFunc    manipulate.RetryFunc
	forcedReadFilter    bson.D
	attributeEncrypter  elemental.AttributeEncrypter
	explain             map[elemental.Identity]map[elemental.Operation]struct{}
	attributeSpecifiers map[elemental.Identity]elemental.AttributeSpecifiable
}

// New returns a new manipulator backed by MongoDB.
func New(url string, db string, options ...Option) (manipulate.TransactionalManipulator, error) {

	cfg := newConfig()
	for _, o := range options {
		o(cfg)
	}

	dialInfo, err := mgo.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("cannot parse mongo url '%s': %s", url, err)
	}

	dialInfo.Database = db
	dialInfo.PoolLimit = cfg.poolLimit
	dialInfo.Username = cfg.username
	dialInfo.Password = cfg.password
	dialInfo.Source = cfg.authsource
	dialInfo.Timeout = cfg.connectTimeout

	if cfg.tlsConfig != nil {
		dialInfo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
			return tls.DialWithDialer(
				&net.Dialer{
					Timeout: dialInfo.Timeout,
				},
				"tcp",
				addr.String(),
				cfg.tlsConfig,
			)
		}
	} else {
		dialInfo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
			return net.DialTimeout("tcp", addr.String(), dialInfo.Timeout)
		}
	}

	session, err := mgo.DialWithInfo(dialInfo)
	if err != nil {
		return nil, fmt.Errorf("cannot connect to mongo url '%s': %s", url, err)
	}

	session.SetSocketTimeout(cfg.socketTimeout)
	session.SetMode(convertReadConsistency(cfg.readConsistency), true)
	session.SetSafe(convertWriteConsistency(cfg.writeConsistency))

	return &mongoManipulator{
		dbName:              db,
		rootSession:         session,
		sharder:             cfg.sharder,
		defaultRetryFunc:    cfg.defaultRetryFunc,
		forcedReadFilter:    cfg.forcedReadFilter,
		attributeEncrypter:  cfg.attributeEncrypter,
		explain:             cfg.explain,
		attributeSpecifiers: cfg.attributeSpecifiers,
	}, nil
}

func (m *mongoManipulator) RetrieveMany(mctx manipulate.Context, dest elemental.Identifiables) error {

	if mctx == nil {
		ctx, cancel := context.WithTimeout(context.Background(), defaultGlobalContextTimeout)
		defer cancel()
		mctx = manipulate.NewContext(ctx)
	}

	sp := tracing.StartTrace(mctx, fmt.Sprintf("manipmongo.retrieve_many.%s", dest.Identity().Category))
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

	// Make filters
	fSharding, err := makeShardingManyFilter(m, mctx, dest.Identity())
	if err != nil {
		return spanErr(sp, err)
	}

	c, closeFunc := m.makeSession(dest.Identity(), mctx.ReadConsistency(), mctx.WriteConsistency())
	defer closeFunc()

	pipe, err := makePipeline(
		attrSpec,
		makePreviousRetriever(c),
		fSharding,
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

	sp.LogFields(log.Object("pipeline", pipe))

	p := c.Pipe(pipe)

	// Query timing limiting
	//
	// This is causing trouble in some request.
	// It seems MaxTimeMS can only be used on tailable cursor
	// whatever that means, but mgo does not handle that correctly
	// and always sets it. For now we should be fine.
	//
	// https://www.mongodb.com/docs/v6.1/reference/command/getMore/
	//
	// Error: cannot set maxTimeMS on getMore command for a non-awaitData cursor
	//
	// if p, err = setMaxTime(mctx.Context(), p); err != nil {
	// 	return spanErr(sp, err)
	// }

	if _, err := RunQuery(
		mctx,
		func() (any, error) {
			if exp := explainIfNeeded(p, bson.D{{Name: "aggregate", Value: pipe}}, dest.Identity(), elemental.OperationRetrieveMany, m.explain); exp != nil {
				if err := exp(); err != nil {
					return nil, manipulate.ErrCannotBuildQuery{Err: fmt.Errorf("retrievemany: unable to explain: %w", err)}
				}
			}
			return nil, p.All(dest)
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

		// backport all default values that are empty.
		if a, ok := o.(elemental.AttributeSpecifiable); ok {
			elemental.ResetDefaultForZeroValues(a)
		}

		// Decrypt attributes if needed.
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
		ctx, cancel := context.WithTimeout(context.Background(), defaultGlobalContextTimeout)
		defer cancel()
		mctx = manipulate.NewContext(ctx)
	}

	sp := tracing.StartTrace(mctx, fmt.Sprintf("manipmongo.retrieve.object.%s", object.Identity().Name))
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

	if oid, ok := objectid.Parse(object.Identifier()); ok {
		filter = append(filter, bson.DocElem{Name: "_id", Value: oid})
	} else {
		filter = append(filter, bson.DocElem{Name: "_id", Value: object.Identifier()})
	}

	var ands []bson.D

	sq, err := makeShardingOneFilter(m, mctx, object)
	if err != nil {
		return spanErr(sp, err)
	}
	if sq != nil {
		ands = append(ands, sq)
	}

	if mctx.Namespace() != "" {
		ands = append(ands, makeNamespaceFilter(mctx))
	}

	if m.forcedReadFilter != nil {
		ands = append(ands, m.forcedReadFilter)
	}

	if len(ands) > 0 {
		filter = bson.D{{Name: "$and", Value: append(ands, filter)}}
	}

	sp.LogFields(log.Object("filter", filter))

	c, closeFunc := m.makeSession(object.Identity(), mctx.ReadConsistency(), mctx.WriteConsistency())
	defer closeFunc()

	q := c.Find(filter)
	if sels := makeFieldsSelector(mctx.Fields(), attrSpec); sels != nil {
		q = q.Select(sels)
	}

	if q, err = setMaxTime(mctx.Context(), q); err != nil {
		return spanErr(sp, err)
	}

	if _, err := RunQuery(
		mctx,
		func() (any, error) {
			if exp := explainIfNeeded(q, filter, object.Identity(), elemental.OperationRetrieve, m.explain); exp != nil {
				if err := exp(); err != nil {
					return nil, manipulate.ErrCannotBuildQuery{Err: fmt.Errorf("retrieve: unable to explain: %w", err)}
				}
			}
			return nil, q.One(object)
		},
		RetryInfo{
			Operation:        elemental.OperationRetrieve,
			Identity:         object.Identity(),
			defaultRetryFunc: m.defaultRetryFunc,
		},
	); err != nil {
		return spanErr(sp, err)
	}

	// backport all default values that are empty.
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
		ctx, cancel := context.WithTimeout(context.Background(), defaultGlobalContextTimeout)
		defer cancel()
		mctx = manipulate.NewContext(ctx)
	}

	sp := tracing.StartTrace(mctx, fmt.Sprintf("manipmongo.create.object.%s", object.Identity().Name))
	sp.LogFields(log.String("object_id", object.Identifier()))
	defer sp.Finish()

	oid := bson.NewObjectId()
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

	if operations, upsert := mctx.(opaquer).Opaque()[opaqueKeyUpsert]; upsert {

		object.SetIdentifier("")

		ops, ok := operations.(bson.M)
		if !ok {
			return spanErr(sp, manipulate.ErrCannotBuildQuery{Err: fmt.Errorf("upsert operations must be of type bson.M")})
		}

		baseOps := bson.M{
			"$set":         object,
			"$setOnInsert": bson.M{"_id": oid},
		}

		if len(ops) > 0 {

			if soi, ok := ops["$setOnInsert"]; ok {
				for k, v := range soi.(bson.M) {
					baseOps["$setOnInsert"].(bson.M)[k] = v
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

		// Filtering
		filter := bson.D{}
		if f := mctx.Filter(); f != nil {
			filter = makeUserFilter(mctx, attrSpec)
		}

		var ands []bson.D

		sq, err := makeShardingOneFilter(m, mctx, object)
		if err != nil {
			return spanErr(sp, err)
		}
		if sq != nil {
			ands = append(ands, sq)
		}

		if mctx.Namespace() != "" {
			ands = append(ands, makeNamespaceFilter(mctx))
		}

		if m.forcedReadFilter != nil {
			ands = append(ands, m.forcedReadFilter)
		}

		if len(ands) > 0 {
			filter = bson.D{{Name: "$and", Value: append(ands, filter)}}
		}

		c, closeFunc := m.makeSession(object.Identity(), mctx.ReadConsistency(), mctx.WriteConsistency())
		defer closeFunc()

		info, err := RunQuery(
			mctx,
			func() (any, error) { return c.Upsert(filter, baseOps) },
			RetryInfo{
				Operation:        elemental.OperationCreate,
				Identity:         object.Identity(),
				defaultRetryFunc: m.defaultRetryFunc,
			},
		)
		if err != nil {
			return spanErr(sp, err)
		}

		switch chinfo := info.(type) {
		case *mgo.ChangeInfo:
			if noid, ok := chinfo.UpsertedId.(bson.ObjectId); ok {
				object.SetIdentifier(noid.Hex())
			}
		}

	} else {

		c, closeFunc := m.makeSession(object.Identity(), mctx.ReadConsistency(), mctx.WriteConsistency())
		defer closeFunc()

		_, err := RunQuery(
			mctx,
			func() (any, error) { return nil, c.Insert(object) },
			RetryInfo{
				Operation:        elemental.OperationCreate,
				Identity:         object.Identity(),
				defaultRetryFunc: m.defaultRetryFunc,
			},
		)

		if err != nil {
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
		ctx, cancel := context.WithTimeout(context.Background(), defaultGlobalContextTimeout)
		defer cancel()
		mctx = manipulate.NewContext(ctx)
	}

	sp := tracing.StartTrace(mctx, fmt.Sprintf("manipmongo.update.object.%s", object.Identity().Name))
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

	var filter bson.D
	if oid, ok := objectid.Parse(object.Identifier()); ok {
		filter = append(filter, bson.DocElem{Name: "_id", Value: oid})
	} else {
		filter = append(filter, bson.DocElem{Name: "_id", Value: object.Identifier()})
	}

	var ands []bson.D

	sq, err := makeShardingOneFilter(m, mctx, object)
	if err != nil {
		return spanErr(sp, err)
	}
	if sq != nil {
		ands = append(ands, sq)
	}

	if mctx.Namespace() != "" {
		ands = append(ands, makeNamespaceFilter(mctx))
	}

	if m.forcedReadFilter != nil {
		ands = append(ands, m.forcedReadFilter)
	}

	if len(ands) > 0 {
		filter = bson.D{{Name: "$and", Value: append(ands, filter)}}
	}

	c, closeFunc := m.makeSession(object.Identity(), mctx.ReadConsistency(), mctx.WriteConsistency())
	defer closeFunc()

	if _, err := RunQuery(
		mctx,
		func() (any, error) { return nil, c.Update(filter, bson.M{"$set": object}) },
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
		ctx, cancel := context.WithTimeout(context.Background(), defaultGlobalContextTimeout)
		defer cancel()
		mctx = manipulate.NewContext(ctx)
	}

	sp := tracing.StartTrace(mctx, fmt.Sprintf("manipmongobject.delete.object.%s", object.Identity().Name))
	sp.LogFields(log.String("object_id", object.Identifier()))
	defer sp.Finish()

	var attrSpec elemental.AttributeSpecifiable
	if m.attributeSpecifiers != nil {
		attrSpec = m.attributeSpecifiers[object.Identity()]
	}

	var filter bson.D
	if f := mctx.Filter(); f != nil {
		filter = makeUserFilter(mctx, attrSpec)
	}

	if oid, ok := objectid.Parse(object.Identifier()); ok {
		filter = append(filter, bson.DocElem{Name: "_id", Value: oid})
	} else {
		filter = append(filter, bson.DocElem{Name: "_id", Value: object.Identifier()})
	}

	var ands []bson.D

	sq, err := makeShardingOneFilter(m, mctx, object)
	if err != nil {
		return spanErr(sp, err)
	}
	if sq != nil {
		ands = append(ands, sq)
	}

	if mctx.Namespace() != "" {
		ands = append(ands, makeNamespaceFilter(mctx))
	}

	if m.forcedReadFilter != nil {
		ands = append(ands, m.forcedReadFilter)
	}

	if len(ands) > 0 {
		filter = bson.D{{Name: "$and", Value: append(ands, filter)}}
	}

	sp.LogFields(log.Object("filter", filter))

	c, closeFunc := m.makeSession(object.Identity(), mctx.ReadConsistency(), mctx.WriteConsistency())
	defer closeFunc()

	if _, err := RunQuery(
		mctx,
		func() (any, error) { return nil, c.Remove(filter) },
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

	// backport all default values that are empty.
	if a, ok := object.(elemental.AttributeSpecifiable); ok {
		elemental.ResetDefaultForZeroValues(a)
	}

	return nil
}

func (m *mongoManipulator) DeleteMany(mctx manipulate.Context, identity elemental.Identity) error {

	if mctx == nil {
		ctx, cancel := context.WithTimeout(context.Background(), defaultGlobalContextTimeout)
		defer cancel()
		mctx = manipulate.NewContext(ctx)
	}

	sp := tracing.StartTrace(mctx, fmt.Sprintf("manipmongo.delete_many.%s", identity.Name))
	defer sp.Finish()

	var attrSpec elemental.AttributeSpecifiable
	if m.attributeSpecifiers != nil {
		attrSpec = m.attributeSpecifiers[identity]
	}

	// Filtering
	filter := bson.D{}
	if f := mctx.Filter(); f != nil {
		filter = makeUserFilter(mctx, attrSpec)
	}

	var ands []bson.D

	sq, err := makeShardingManyFilter(m, mctx, identity)
	if err != nil {
		return spanErr(sp, err)
	}
	if sq != nil {
		ands = append(ands, sq)
	}

	if mctx.Namespace() != "" {
		ands = append(ands, makeNamespaceFilter(mctx))
	}

	if m.forcedReadFilter != nil {
		ands = append(ands, m.forcedReadFilter)
	}

	if len(ands) > 0 {
		filter = bson.D{{Name: "$and", Value: append(ands, filter)}}
	}

	sp.LogFields(log.Object("filter", filter))

	c, closeFunc := m.makeSession(identity, mctx.ReadConsistency(), mctx.WriteConsistency())
	defer closeFunc()

	if _, err := RunQuery(
		mctx,
		func() (any, error) { return c.RemoveAll(filter) },
		RetryInfo{
			Operation:        elemental.OperationDelete, // we miss DeleteMany
			Identity:         identity,
			defaultRetryFunc: m.defaultRetryFunc,
		},
	); err != nil {
		sp.SetTag("error", true)
		sp.LogFields(log.Error(err))
		return err
	}

	return nil
}

type countRes struct {
	Count int `bson:"_count"`
}

func (m *mongoManipulator) Count(mctx manipulate.Context, identity elemental.Identity) (int, error) {

	if mctx == nil {
		ctx, cancel := context.WithTimeout(context.Background(), defaultGlobalContextTimeout)
		defer cancel()
		mctx = manipulate.NewContext(ctx)
	}

	sp := tracing.StartTrace(mctx, fmt.Sprintf("manipmongo.count.%s", identity.Category))
	defer sp.Finish()

	var attrSpec elemental.AttributeSpecifiable
	if m.attributeSpecifiers != nil {
		attrSpec = m.attributeSpecifiers[identity]
	}

	fSharding, err := makeShardingManyFilter(m, mctx, identity)
	if err != nil {
		return 0, spanErr(sp, err)
	}

	c, closeFunc := m.makeSession(identity, mctx.ReadConsistency(), mctx.WriteConsistency())
	defer closeFunc()

	pipe, err := makePipeline(
		attrSpec,
		makePreviousRetriever(c),
		fSharding,
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

	pipe = append(pipe, bson.M{"$count": "_count"})

	sp.LogFields(log.Object("pipeline", pipe))

	p := c.Pipe(pipe)

	// Query timing limiting
	if p, err = setMaxTime(mctx.Context(), p); err != nil {
		return 0, spanErr(sp, err)
	}

	res := []*countRes{}

	_, err = RunQuery(
		mctx,
		func() (any, error) {
			if exp := explainIfNeeded(p, bson.D{{Name: "aggregate", Value: pipe}}, identity, elemental.OperationInfo, m.explain); exp != nil {
				if err := exp(); err != nil {
					return nil, manipulate.ErrCannotBuildQuery{Err: fmt.Errorf("count: unable to explain: %w", err)}
				}
			}
			return nil, p.All(&res)
		},
		RetryInfo{
			Operation:        elemental.OperationInfo,
			Identity:         identity,
			defaultRetryFunc: m.defaultRetryFunc,
		},
	)
	if err != nil {
		return 0, spanErr(sp, err)
	}

	switch len(res) {
	case 0:
		return 0, nil
	case 1:
		return res[0].Count, nil
	default:
		return 0, manipulate.ErrCannotExecuteQuery{Err: fmt.Errorf("count: invalid count result len: %d. must be 1", len(res))}
	}
}

func (m *mongoManipulator) Commit(id manipulate.TransactionID) error { return nil }

func (m *mongoManipulator) Abort(id manipulate.TransactionID) bool { return true }

func (m *mongoManipulator) Ping(timeout time.Duration) error {

	errChannel := make(chan error, 1)

	go func() {
		errChannel <- m.rootSession.Ping()
	}()

	select {
	case <-time.After(timeout):
		return fmt.Errorf("timeout")
	case err := <-errChannel:
		return err
	}
}

func (m *mongoManipulator) makeSession(
	identity elemental.Identity,
	readConsistency manipulate.ReadConsistency,
	writeConsistency manipulate.WriteConsistency,
) (*mgo.Collection, func()) {

	session := m.rootSession.Copy()

	if mrc := convertReadConsistency(readConsistency); mrc != -1 {
		session.SetMode(mrc, true)
	}

	session.SetSafe(convertWriteConsistency(writeConsistency))

	return session.DB(m.dbName).C(identity.Name), session.Close
}
