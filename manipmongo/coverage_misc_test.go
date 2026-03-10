package manipmongo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.acuvity.ai/elemental"
	"go.acuvity.ai/manipulate"
	mongobson "go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mongooptions "go.mongodb.org/mongo-driver/v2/mongo/options"
)

type mongoBadConvertSharder struct{}

func (mongoBadConvertSharder) Shard(manipulate.TransactionalManipulator, manipulate.Context, elemental.Identifiable) error {
	return nil
}
func (mongoBadConvertSharder) OnShardedWrite(manipulate.TransactionalManipulator, manipulate.Context, elemental.Operation, elemental.Identifiable) error {
	return nil
}
func (mongoBadConvertSharder) FilterOne(manipulate.TransactionalManipulator, manipulate.Context, elemental.Identifiable) (mongobson.D, error) {
	return mongobson.D{{Key: "bad", Value: func() {}}}, nil
}
func (mongoBadConvertSharder) FilterMany(manipulate.TransactionalManipulator, manipulate.Context, elemental.Identity) (mongobson.D, error) {
	return mongobson.D{{Key: "bad", Value: func() {}}}, nil
}

type plainTimeoutErr struct{}

func (plainTimeoutErr) Error() string { return "plain timeout" }
func (plainTimeoutErr) Timeout() bool { return true }

func TestCloneBSONAdditionalBranches(t *testing.T) {
	if _, err := cloneBSONValue[mongobson.D](mongobson.D{{Key: "bad", Value: func() {}}}); err == nil {
		t.Fatalf("expected clone conversion error")
	}
	if _, err := cloneBSONValue[int](mongobson.M{"a": 1}); err == nil {
		t.Fatalf("expected unmarshal error for incompatible target type")
	}
}

func TestRunQueryMongoAndHandleErrorAdditionalBranches(t *testing.T) {
	sentinel := errors.New("sentinel")

	_, err := runQueryMongo(
		manipulate.NewContext(
			context.Background(),
			manipulate.ContextOptionRetryFunc(func(manipulate.RetryInfo) error { return sentinel }),
		),
		func() (any, error) { return nil, mongo.CommandError{Code: 6, Message: "host unreachable"} },
		RetryInfo{},
	)
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected retry func sentinel error, got %v", err)
	}

	_, err = runQueryMongo(
		manipulate.NewContext(context.Background()),
		func() (any, error) { return nil, mongo.CommandError{Code: 6, Message: "host unreachable"} },
		RetryInfo{defaultRetryFunc: func(manipulate.RetryInfo) error { return sentinel }},
	)
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected default retry func sentinel error, got %v", err)
	}

	doneCtx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	time.Sleep(time.Millisecond)
	cancel()
	_, err = runQueryMongo(
		manipulate.NewContext(
			doneCtx,
			manipulate.ContextOptionRetryFunc(func(manipulate.RetryInfo) error { return nil }),
		),
		func() (any, error) { return nil, mongo.CommandError{Code: 6, Message: "host unreachable"} },
		RetryInfo{},
	)
	if err == nil || !manipulate.IsCannotExecuteQueryError(err) {
		t.Fatalf("expected cannot execute query error on done context, got %v", err)
	}

	dupErr := mongo.WriteException{
		WriteErrors: []mongo.WriteError{{Code: 11000, Message: "dup"}},
	}
	if err := HandleQueryErrorMongo(dupErr); !manipulate.IsConstraintViolationError(err) {
		t.Fatalf("expected duplicate key to map to constraint violation, got %v", err)
	}

	if err := HandleQueryErrorMongo(errors.New("connection reset by peer")); err == nil {
		t.Fatalf("expected connection reset to map to a non-nil query error")
	}

	if err := HandleQueryErrorMongo(plainTimeoutErr{}); !manipulate.IsCannotCommunicateError(err) {
		t.Fatalf("expected timeout-like net error to map to cannot communicate, got %v", err)
	}

	if _, err := convertUpsertOperationsToMongo(mongobson.M(nil)); err != nil {
		t.Fatalf("expected nil bson map conversion to succeed, got %v", err)
	}
}

func TestHelpersAdditionalBranchesWithMemongo(t *testing.T) {
	uri, db := requireMemongo(t)
	manipulatorAPI, err := New(uri, db)
	if err != nil {
		t.Fatalf("unable to create mongo manipulator: %v", err)
	}

	identity := elemental.MakeIdentity("helperextra", "helperextras")

	existsBefore, err := DoesDatabaseExist(manipulatorAPI)
	if err != nil {
		t.Fatalf("does database exist failed: %v", err)
	}
	if existsBefore {
		t.Fatalf("expected database to not exist yet")
	}

	if err := CreateCollection(manipulatorAPI, identity, mongooptions.CreateCollection()); err != nil {
		t.Fatalf("create collection failed: %v", err)
	}
	existsAfter, err := DoesDatabaseExist(manipulatorAPI)
	if err != nil {
		t.Fatalf("does database exist after create failed: %v", err)
	}
	if !existsAfter {
		t.Fatalf("expected database to exist after collection creation")
	}

	if err := EnsureIndex(manipulatorAPI, identity, mongo.IndexModel{Keys: mongobson.D{{Key: "$bad", Value: int32(1)}}}); err == nil {
		t.Fatalf("expected ensure index error on invalid key")
	}
	if err := CreateIndex(manipulatorAPI, identity, mongo.IndexModel{Keys: mongobson.D{{Key: "$bad", Value: int32(1)}}}); err == nil {
		t.Fatalf("expected create index error on invalid key")
	}
	if err := DeleteIndex(manipulatorAPI, identity, "does_not_exist"); err == nil {
		t.Fatalf("expected delete index error on non-existent index")
	}

	if err := DropDatabase(manipulatorAPI); err != nil {
		t.Fatalf("drop database failed: %v", err)
	}

	dbHandle, err := GetDatabase(manipulatorAPI)
	if err != nil || dbHandle == nil {
		t.Fatalf("expected canonical GetDatabase success, db=%v err=%v", dbHandle, err)
	}

	mongoManipulator, err := New(uri, db+"_mongo", OptionForceReadFilter(mongobson.D{}))
	if err != nil {
		t.Fatalf("unable to create mongo manipulator: %v", err)
	}
	if err := CreateIndex(mongoManipulator, identity); err != nil {
		t.Fatalf("expected create indexes with no models to succeed, got %v", err)
	}
	if err := EnsureIndex(mongoManipulator, identity); err != nil {
		t.Fatalf("expected ensure indexes with no models to succeed, got %v", err)
	}

	if err := CreateCollection(mongoManipulator, identity, mongooptions.CreateCollection()); err != nil {
		t.Fatalf("create collection mongo failed: %v", err)
	}
	if err := CreateCollection(mongoManipulator, identity, mongooptions.CreateCollection()); err == nil {
		t.Fatalf("expected duplicate create collection mongo error")
	}

	if err := CreateIndex(
		mongoManipulator,
		identity,
		mongo.IndexModel{Keys: mongobson.D{{Key: "$bad", Value: 1}}},
	); err == nil {
		t.Fatalf("expected create indexes mongo invalid-key error")
	}

	if err := EnsureIndex(
		mongoManipulator,
		identity,
		mongo.IndexModel{Keys: mongobson.D{{Key: "$bad", Value: 1}}},
	); err == nil {
		t.Fatalf("expected ensure indexes mongo invalid-key error")
	}

	badTypeErr := fmt.Errorf("bad type")
	if !isMongoIndexConflictError(mongo.CommandError{Code: 86, Message: "IndexKeySpecsConflict"}) {
		t.Fatalf("expected code 86 to be classified as conflict")
	}
	if isMongoIndexConflictError(badTypeErr) && strings.Contains(badTypeErr.Error(), "index") {
		t.Fatalf("unexpected conflict classification")
	}
}

func TestMongoManipulatorInternalHelperBranchesWithMemongo(t *testing.T) {
	uri, db := requireMemongo(t)
	mapi, err := New(uri, db, OptionForceReadFilter(mongobson.D{}))
	if err != nil {
		t.Fatalf("unable to create mongo manipulator: %v", err)
	}
	m := mapi.(*mongoDriverManipulator)

	if coll := m.makeCollection(mongoIntegrationIdentity, manipulate.ReadConsistencyNearest, manipulate.WriteConsistencyStrongest); coll == nil {
		t.Fatalf("expected non-nil collection")
	}

	badSharderManipulator := &mongoDriverManipulator{sharder: mongoBadConvertSharder{}}
	oneFilter, err := badSharderManipulator.makeShardingOneFilter(manipulate.NewContext(context.Background()), newMongoIntegrationObject("x"))
	if err != nil {
		t.Fatalf("expected sharding one filter to be returned, got err=%v", err)
	}
	if len(oneFilter) == 0 {
		t.Fatalf("expected non-empty sharding one filter")
	}

	manyFilter, err := badSharderManipulator.makeShardingManyFilter(manipulate.NewContext(context.Background()), mongoIntegrationIdentity)
	if err != nil {
		t.Fatalf("expected sharding many filter to be returned, got err=%v", err)
	}
	if len(manyFilter) == 0 {
		t.Fatalf("expected non-empty sharding many filter")
	}

	pipeline, err := makePipeline(nil, func(mongobson.ObjectID) (mongobson.M, error) { return mongobson.M{}, nil }, manyFilter, nil, nil, nil, nil, "", 0, nil)
	if err != nil {
		t.Fatalf("expected pipeline construction to succeed, got %v", err)
	}
	if len(pipeline) == 0 {
		t.Fatalf("expected non-empty pipeline")
	}

	coll := m.client.Database(m.dbName).Collection(mongoIntegrationIdentity.Name)
	prev := makePreviousRetrieverMongo(coll, context.Background())
	if _, err := prev(mongobson.NewObjectID()); err == nil {
		t.Fatalf("expected not-found error from previous retriever")
	}
}
