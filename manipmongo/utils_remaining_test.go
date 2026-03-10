package manipmongo

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.acuvity.ai/elemental"
	"go.acuvity.ai/manipulate"
	bson "go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mongooptions "go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestWorkerA2MakePreviousRetrieverMongoCollectionBranch(t *testing.T) {
	uri, db := requireMemongo(t)

	client, err := mongo.Connect(mongooptions.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("unable to connect mongo client: %v", err)
	}
	t.Cleanup(func() {
		_ = client.Disconnect(context.Background())
	})

	coll := client.Database(db).Collection("worker_a2_previous")
	id := bson.NewObjectID()
	if _, err := coll.InsertOne(context.Background(), bson.M{"_id": id, "name": "alpha"}); err != nil {
		t.Fatalf("unable to seed collection: %v", err)
	}

	retriever := makePreviousRetriever(coll)
	doc, err := retriever(id)
	if err != nil {
		t.Fatalf("unexpected retriever error: %v", err)
	}
	if doc["name"] != "alpha" {
		t.Fatalf("unexpected retrieved doc: %#v", doc)
	}
}

func TestWorkerA2MakePreviousRetrieverMongoCollectionNotFound(t *testing.T) {
	uri, db := requireMemongo(t)

	client, err := mongo.Connect(mongooptions.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("unable to connect mongo client: %v", err)
	}
	t.Cleanup(func() {
		_ = client.Disconnect(context.Background())
	})

	retriever := makePreviousRetriever(client.Database(db).Collection("worker_a2_previous_missing"))
	if _, err := retriever(bson.NewObjectID()); err == nil {
		t.Fatalf("expected retriever error for missing document")
	}
}

func TestWorkerA2MakeShardingFiltersWithNilSharder(t *testing.T) {
	m := &mongoDriverManipulator{}
	mctx := manipulate.NewContext(context.Background())

	many, err := makeShardingManyFilter(m, mctx, elemental.MakeIdentity("thing", "things"))
	if err != nil {
		t.Fatalf("unexpected error for nil sharder many filter: %v", err)
	}
	if many != nil {
		t.Fatalf("expected nil many filter for nil sharder, got %#v", many)
	}

	one, err := makeShardingOneFilter(m, mctx, nil)
	if err != nil {
		t.Fatalf("unexpected error for nil sharder one filter: %v", err)
	}
	if one != nil {
		t.Fatalf("expected nil one filter for nil sharder, got %#v", one)
	}
}

func TestWorkerA2MakePipelineNamespaceAndForcedStages(t *testing.T) {
	pipe, err := makePipeline(
		nil,
		nil,
		bson.D{{Key: "shard", Value: "s"}},
		bson.D{{Key: "namespace", Value: "n"}},
		bson.D{{Key: "forced", Value: true}},
		nil,
		nil,
		"",
		0,
		nil,
	)
	if err != nil {
		t.Fatalf("unexpected pipeline error: %v", err)
	}
	if len(pipe) < 3 {
		t.Fatalf("expected at least 3 stages, got %#v", pipe)
	}

	match1, ok := pipe[0].(bson.D)
	if !ok || len(match1) == 0 || match1[0].Key != "$match" {
		t.Fatalf("expected first stage match, got %#v", pipe[0])
	}
	match2, ok := pipe[1].(bson.D)
	if !ok || len(match2) == 0 || match2[0].Key != "$match" {
		t.Fatalf("expected second stage match, got %#v", pipe[1])
	}
	match3, ok := pipe[2].(bson.D)
	if !ok || len(match3) == 0 || match3[0].Key != "$match" {
		t.Fatalf("expected third stage match, got %#v", pipe[2])
	}
}

func TestWorkerA2MakePipelineAfterRetrieverErrorBranches(t *testing.T) {
	after := bson.NewObjectID().Hex()

	_, err := makePipeline(
		nil,
		func(bson.ObjectID) (bson.M, error) { return nil, errors.New("previous failed") },
		nil,
		nil,
		nil,
		nil,
		[]string{"name"},
		after,
		0,
		nil,
	)
	if err == nil {
		t.Fatalf("expected retriever failure")
	}
	var cannotExecute manipulate.ErrCannotExecuteQuery
	if !errors.As(err, &cannotExecute) {
		t.Fatalf("expected cannot execute query on retriever error, got %T (%v)", err, err)
	}

	_, err = makePipeline(
		nil,
		func(bson.ObjectID) (bson.M, error) { return nil, nil },
		nil,
		nil,
		nil,
		nil,
		[]string{"name"},
		after,
		0,
		nil,
	)
	if err == nil {
		t.Fatalf("expected retriever nil-doc failure")
	}
	if !errors.As(err, &cannotExecute) {
		t.Fatalf("expected cannot execute query on nil previous doc, got %T (%v)", err, err)
	}
}

func TestWorkerA2MakePipelineDescendingAndAfterIDMatchBranch(t *testing.T) {
	afterID := bson.NewObjectID()
	pipe, err := makePipeline(
		nil,
		func(id bson.ObjectID) (bson.M, error) {
			return bson.M{"name": "zeta"}, nil
		},
		nil,
		nil,
		nil,
		nil,
		[]string{"-name", "_id"},
		afterID.Hex(),
		0,
		nil,
	)
	if err != nil {
		t.Fatalf("unexpected pipeline error: %v", err)
	}

	var sortDoc bson.D
	var matchAfter bson.D
	for _, stage := range pipe {
		doc, ok := stage.(bson.D)
		if !ok || len(doc) == 0 {
			continue
		}
		switch doc[0].Key {
		case "$sort":
			sortDoc, _ = doc[0].Value.(bson.D)
		case "$match":
			matchAfter, _ = doc[0].Value.(bson.D)
		}
	}

	if len(sortDoc) == 0 {
		t.Fatalf("expected sort stage in pipeline, got %#v", pipe)
	}
	if len(sortDoc) < 2 {
		t.Fatalf("expected two sort fields, got %#v", sortDoc)
	}
	if sortDoc[0].Key != "name" || sortDoc[0].Value != -1 {
		t.Fatalf("expected descending name sort, got %#v", sortDoc)
	}
	if sortDoc[1].Key != "_id" || sortDoc[1].Value != 1 {
		t.Fatalf("expected ascending _id sort, got %#v", sortDoc)
	}

	if len(matchAfter) == 0 || matchAfter[0].Key != "$and" {
		t.Fatalf("expected after match stage with $and, got %#v", matchAfter)
	}
	andDocs, ok := matchAfter[0].Value.([]bson.D)
	if !ok || len(andDocs) != 2 {
		t.Fatalf("expected two and-docs for order fields, got %#v", matchAfter[0].Value)
	}

	hasIDGt := false
	for _, d := range andDocs {
		if len(d) == 0 || d[0].Key != "_id" {
			continue
		}
		idDoc, ok := d[0].Value.(bson.D)
		if !ok || len(idDoc) == 0 || idDoc[0].Key != "$gt" {
			continue
		}
		if oid, ok := idDoc[0].Value.(bson.ObjectID); ok && oid == afterID {
			hasIDGt = true
		}
	}
	if !hasIDGt {
		t.Fatalf("expected _id $gt condition in after match, got %#v", andDocs)
	}
}

func TestWorkerA2HandleQueryErrorConnectionStringPath(t *testing.T) {
	got := HandleQueryError(errors.New("not master"))
	if !manipulate.IsCannotCommunicateError(got) {
		t.Fatalf("expected cannot communicate error, got %T (%v)", got, got)
	}
}

func TestWorkerA2InvalidQuery51091Branch(t *testing.T) {
	ok, gotErr := invalidQuery(mongo.CommandError{
		Code:    51091,
		Message: errInvalidQueryInvalidRegex,
	})
	if !ok {
		t.Fatalf("expected invalid query branch for code 51091")
	}
	var invalid manipulate.ErrInvalidQuery
	if !errors.As(gotErr, &invalid) {
		t.Fatalf("expected invalid query error type, got %T (%v)", gotErr, gotErr)
	}
	if !invalid.DueToFilter {
		t.Fatalf("expected DueToFilter=true")
	}
}

func TestWorkerA2IsConnectionErrorNilAndNetworkLabel(t *testing.T) {
	if isConnectionError(nil) {
		t.Fatalf("expected nil input to not be a connection error")
	}

	labeled := mongo.CommandError{
		Code:    999,
		Message: "network labeled",
		Labels:  []string{"NetworkError"},
	}
	if !isConnectionError(labeled) {
		t.Fatalf("expected labeled network error to be a connection error")
	}
}

func TestWorkerA2ExplainIfNeededIdentityMissingBranch(t *testing.T) {
	identity := elemental.MakeIdentity("target", "targets")
	other := elemental.MakeIdentity("other", "others")

	callback := explainIfNeeded(
		&workerAExplainableStub{},
		bson.D{{Key: "k", Value: "v"}},
		identity,
		elemental.OperationRetrieve,
		map[elemental.Identity]map[elemental.Operation]struct{}{
			other: {elemental.OperationRetrieve: {}},
		},
	)
	if callback != nil {
		t.Fatalf("expected nil callback when identity is missing from explain map")
	}
}

func TestWorkerA2ExplainExplainErrorPath(t *testing.T) {
	err := explain(
		&workerAExplainableStub{err: errors.New("boom")},
		elemental.OperationRetrieve,
		elemental.MakeIdentity("thing", "things"),
		bson.D{{Key: "k", Value: "v"}},
	)
	if err == nil || !strings.Contains(err.Error(), "unable to explain") {
		t.Fatalf("expected wrapped explain error, got %v", err)
	}
}

func TestWorkerA2SetMaxTimeDeadlineSuccessPath(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(750*time.Millisecond))
	defer cancel()

	updated, err := setMaxTime(ctx, workerAMaxTimeQuery{})
	if err != nil {
		t.Fatalf("unexpected setMaxTime error with active deadline: %v", err)
	}
	if updated.last <= 0 {
		t.Fatalf("expected positive max time from deadline, got %s", updated.last)
	}
	if updated.last > time.Second {
		t.Fatalf("expected max time close to deadline, got %s", updated.last)
	}
}
