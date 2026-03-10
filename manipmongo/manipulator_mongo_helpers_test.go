package manipmongo

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.acuvity.ai/elemental"
	testmodel "go.acuvity.ai/elemental/test/model"
	"go.acuvity.ai/manipulate"
	mongobson "go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mongooptions "go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

func TestConvertReadPreferenceMongoMappings(t *testing.T) {
	tests := []struct {
		in   manipulate.ReadConsistency
		want *readpref.ReadPref
	}{
		{manipulate.ReadConsistencyEventual, readpref.SecondaryPreferred()},
		{manipulate.ReadConsistencyMonotonic, readpref.PrimaryPreferred()},
		{manipulate.ReadConsistencyNearest, readpref.Nearest()},
		{manipulate.ReadConsistencyStrong, readpref.Primary()},
		{manipulate.ReadConsistencyWeakest, readpref.SecondaryPreferred()},
		{manipulate.ReadConsistencyDefault, nil},
	}

	for _, tt := range tests {
		got := convertReadPreferenceMongo(tt.in)
		if tt.want == nil {
			if got != nil {
				t.Fatalf("expected nil read preference for %v, got %v", tt.in, got)
			}
			continue
		}
		if got == nil || got.Mode() != tt.want.Mode() {
			t.Fatalf("unexpected read preference for %v: got=%v want=%v", tt.in, got, tt.want)
		}
	}
}

func TestRedactMongoURIEdgeCases(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "empty username with password gets placeholder username",
			raw:  "mongodb://:secret@localhost:27017/db",
			want: "mongodb://redacted:REDACTED@localhost:27017/db",
		},
		{
			name: "username without password remains non-secret",
			raw:  "mongodb://user@localhost:27017/db",
			want: "mongodb://user@localhost:27017/db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := redactMongoURI(tt.raw)
			if got != tt.want {
				t.Fatalf("unexpected redacted URI: got=%q want=%q", got, tt.want)
			}
		})
	}
}

func TestConvertWriteConcernMongoMappings(t *testing.T) {
	if wc := convertWriteConcernMongo(manipulate.WriteConsistencyNone); wc == nil || wc.Acknowledged() {
		t.Fatalf("expected unacknowledged write concern for none")
	}

	if wc := convertWriteConcernMongo(manipulate.WriteConsistencyStrong); wc == nil || !wc.Acknowledged() {
		t.Fatalf("expected acknowledged majority write concern for strong")
	}

	if wc := convertWriteConcernMongo(manipulate.WriteConsistencyStrongest); wc == nil || !wc.Acknowledged() || wc.Journal == nil || !*wc.Journal {
		t.Fatalf("expected journaled acknowledged write concern for strongest")
	}

	if wc := convertWriteConcernMongo(manipulate.WriteConsistencyDefault); wc == nil || !wc.Acknowledged() {
		t.Fatalf("expected default write concern to be acknowledged")
	}
}

func TestMaxTimeFromContext(t *testing.T) {
	noDeadline, err := maxTimeFromContext(context.Background())
	if err != nil {
		t.Fatalf("unexpected error on context without deadline: %v", err)
	}
	if noDeadline <= 0 {
		t.Fatalf("expected positive default max time, got %v", noDeadline)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	deadlineTime, err := maxTimeFromContext(ctx)
	if err != nil {
		t.Fatalf("unexpected error on context with deadline: %v", err)
	}
	if deadlineTime <= 0 {
		t.Fatalf("expected positive deadline duration, got %v", deadlineTime)
	}

	canceled, cancelNow := context.WithTimeout(context.Background(), time.Second)
	cancelNow()
	if _, err := maxTimeFromContext(canceled); err == nil {
		t.Fatalf("expected error for canceled context")
	}
}

func TestRunQueryMongoAndHelpers(t *testing.T) {
	calls := 0
	retries := 0
	mctx := manipulate.NewContext(context.Background(), manipulate.ContextOptionRetryFunc(func(manipulate.RetryInfo) error {
		retries++
		return nil
	}))

	_, err := runQueryMongo(
		mctx,
		func() (any, error) {
			calls++
			if calls == 1 {
				return nil, mongo.CommandError{Code: 6, Message: "host unreachable"}
			}
			return "ok", nil
		},
		RetryInfo{},
	)
	if err != nil {
		t.Fatalf("unexpected runQueryMongo error: %v", err)
	}
	if retries != 1 {
		t.Fatalf("expected exactly one retry, got %d", retries)
	}

	_, err = runQueryMongo(
		manipulate.NewContext(context.Background()),
		func() (any, error) {
			return nil, errors.New("boom")
		},
		RetryInfo{},
	)
	if !manipulate.IsCannotExecuteQueryError(err) {
		t.Fatalf("expected cannot execute error, got %v", err)
	}

	dst := mongobson.M{"a": 1}
	if err := mergeSetOnInsertOperationsMongo(dst, mongobson.M{"b": 2}); err != nil {
		t.Fatalf("unexpected mergeSetOnInsertOperationsMongo error: %v", err)
	}
	if dst["b"] != 2 {
		t.Fatalf("mergeSetOnInsertOperationsMongo should copy entries into dst")
	}
}

func TestMongoFilterAndPipelineHelpers(t *testing.T) {
	m := &mongoDriverManipulator{
		sharder: &integrationMongoSharder{tenant: "tenant-conv"},
	}
	task := newTestTask("conv")

	filterOne, err := m.makeShardingOneFilter(manipulate.NewContext(context.Background()), task)
	if err != nil {
		t.Fatalf("unexpected makeShardingOneFilter error: %v", err)
	}
	if len(filterOne) == 0 {
		t.Fatalf("expected sharding filter one")
	}

	filterMany, err := m.makeShardingManyFilter(manipulate.NewContext(context.Background()), testmodel.TaskIdentity)
	if err != nil {
		t.Fatalf("unexpected makeShardingManyFilter error: %v", err)
	}
	if len(filterMany) == 0 {
		t.Fatalf("expected sharding filter many")
	}

	pipeline, err := makePipeline(
		nil,
		func(mongobson.ObjectID) (mongobson.M, error) { return mongobson.M{}, nil },
		filterMany,
		nil,
		nil,
		nil,
		nil,
		"",
		0,
		nil,
	)
	if err != nil {
		t.Fatalf("unexpected makePipeline error: %v", err)
	}
	if len(pipeline) != 1 {
		t.Fatalf("expected one pipeline stage, got %d", len(pipeline))
	}

	firstStage, ok := pipeline[0].(mongobson.D)
	if !ok {
		t.Fatalf("expected bson.D stage type, got %T", pipeline[0])
	}
	if len(firstStage) == 0 || firstStage[0].Key != "$match" {
		t.Fatalf("unexpected first stage: %#v", firstStage)
	}
}

func TestMongoIndexHelperUtilities(t *testing.T) {
	identity := elemental.MakeIdentity("helpers", "helpers")

	model, name, ttl, err := normalizeMongoIndexModel(
		identity,
		0,
		mongo.IndexModel{
			Keys:    mongobson.D{{Key: "name", Value: 1}},
			Options: mongooptions.Index().SetExpireAfterSeconds(30),
		},
	)
	if err != nil {
		t.Fatalf("unexpected normalize error: %v", err)
	}
	if model.Options == nil {
		t.Fatalf("expected index options to be present")
	}
	if name == "" || ttl == nil || *ttl != 30 {
		t.Fatalf("unexpected normalize output name=%q ttl=%v", name, ttl)
	}

	applied, err := applyMongoIndexOptions(mongooptions.Index().SetName("idx"))
	if err != nil {
		t.Fatalf("unexpected apply options error: %v", err)
	}
	if applied.Name == nil || *applied.Name != "idx" {
		t.Fatalf("expected index name idx, got %#v", applied.Name)
	}

	if !isMongoIndexConflictError(mongo.CommandError{Code: 85, Message: "IndexOptionsConflict"}) {
		t.Fatalf("expected code 85 to be considered index conflict")
	}
	if !isMongoIndexConflictError(fmt.Errorf("already exists with different options")) {
		t.Fatalf("expected conflict message match")
	}
	if isMongoIndexConflictError(errors.New("other")) {
		t.Fatalf("unexpected conflict classification for generic error")
	}
}

func TestMongoOperationContext(t *testing.T) {
	ctx, cancel, err := mongoOperationContext(context.Background())
	if err != nil {
		t.Fatalf("unexpected operation context error: %v", err)
	}
	defer cancel()

	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatalf("expected operation context to have a deadline")
	}
	if remaining := time.Until(deadline); remaining <= 0 {
		t.Fatalf("expected positive remaining deadline, got %v", remaining)
	}

	canceled, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	if _, cancel, err := mongoOperationContext(canceled); err == nil {
		if cancel != nil {
			cancel()
		}
		t.Fatalf("expected canceled context to return an error")
	}
}

func TestMongoDriverManipulatorDefaultConsistencyAccessors(t *testing.T) {
	m := &mongoDriverManipulator{}
	m.setDefaultConsistency(manipulate.ReadConsistencyNearest, manipulate.WriteConsistencyStrongest)

	read, write := m.defaultConsistency()
	if read != manipulate.ReadConsistencyNearest {
		t.Fatalf("unexpected read consistency: %v", read)
	}
	if write != manipulate.WriteConsistencyStrongest {
		t.Fatalf("unexpected write consistency: %v", write)
	}
}

func TestMongoDriverManipulatorComposeFilters(t *testing.T) {
	filter := mongobson.D{identifierFilterElement("not-an-object-id")}
	if len(filter) != 1 || filter[0].Key != "_id" {
		t.Fatalf("unexpected identifier filter: %#v", filter)
	}
	if _, ok := filter[0].Value.(string); !ok {
		t.Fatalf("expected string id fallback, got %T", filter[0].Value)
	}

	parsed := identifierFilterElement(mongobson.NewObjectID().Hex())
	if _, ok := parsed.Value.(mongobson.ObjectID); !ok {
		t.Fatalf("expected parsed object id, got %T", parsed.Value)
	}

	base := mongobson.D{{Key: "name", Value: "demo"}}
	composed := composeAndFilter(base, mongobson.D{{Key: "tenant", Value: "acme"}})
	if len(composed) != 1 || composed[0].Key != "$and" {
		t.Fatalf("unexpected composed filter: %#v", composed)
	}

	unchanged := composeAndFilter(base)
	if len(unchanged) != len(base) || unchanged[0].Key != base[0].Key {
		t.Fatalf("expected unchanged filter without extra clauses, got %#v", unchanged)
	}
}

func TestWorkerBSharderErrorEarlyReturnsAcrossManipulatorMethods(t *testing.T) {
	expectedErr := errors.New("sharder failed")
	m := &mongoDriverManipulator{
		client:  &mongo.Client{},
		dbName:  "workerb",
		sharder: &workerATestSharder{err: expectedErr},
	}
	mctx := manipulate.NewContext(context.Background())
	task := testmodel.NewTask()
	task.SetIdentifier(mongobson.NewObjectID().Hex())

	var tasks testmodel.TasksList

	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "RetrieveMany",
			call: func() error { return m.RetrieveMany(mctx, &tasks) },
		},
		{
			name: "Retrieve",
			call: func() error { return m.Retrieve(mctx, task) },
		},
		{
			name: "Create",
			call: func() error { return m.Create(mctx, testmodel.NewTask()) },
		},
		{
			name: "Update",
			call: func() error { return m.Update(mctx, task) },
		},
		{
			name: "Delete",
			call: func() error { return m.Delete(mctx, task) },
		},
		{
			name: "DeleteMany",
			call: func() error { return m.DeleteMany(mctx, testmodel.TaskIdentity) },
		},
		{
			name: "Count",
			call: func() error {
				_, err := m.Count(mctx, testmodel.TaskIdentity)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call()
			if err == nil {
				t.Fatalf("expected sharder error")
			}
			var cannotBuild manipulate.ErrCannotBuildQuery
			if !errors.As(err, &cannotBuild) {
				t.Fatalf("expected manipulate.ErrCannotBuildQuery, got %T (%v)", err, err)
			}
		})
	}
}

func TestWorkerBMakeShardingManyFilterNilAndErrorBranches(t *testing.T) {
	mctx := manipulate.NewContext(context.Background())

	noSharder := &mongoDriverManipulator{}
	filter, err := noSharder.makeShardingManyFilter(mctx, testmodel.TaskIdentity)
	if err != nil {
		t.Fatalf("expected nil error with nil sharder, got: %v", err)
	}
	if filter != nil {
		t.Fatalf("expected nil filter with nil sharder, got: %#v", filter)
	}

	withErr := &mongoDriverManipulator{
		sharder: &workerATestSharder{err: errors.New("filter-many error")},
	}
	_, err = withErr.makeShardingManyFilter(mctx, testmodel.TaskIdentity)
	if err == nil {
		t.Fatalf("expected sharding filter error")
	}
	var cannotBuild manipulate.ErrCannotBuildQuery
	if !errors.As(err, &cannotBuild) {
		t.Fatalf("expected manipulate.ErrCannotBuildQuery, got %T (%v)", err, err)
	}
}

func TestWorkerBRetrieveResetsDefaultsAfterDecode(t *testing.T) {
	uri, db := requireMemongo(t)

	mapi, err := New(uri, db)
	if err != nil {
		t.Fatalf("unexpected constructor error: %v", err)
	}

	m, ok := mapi.(*mongoDriverManipulator)
	if !ok {
		t.Fatalf("expected *mongoDriverManipulator, got %T", mapi)
	}

	coll := m.client.Database(m.dbName).Collection(testmodel.TaskIdentity.Name)
	oid := mongobson.NewObjectID()
	if _, err := coll.InsertOne(context.Background(), mongobson.M{
		"_id":  oid,
		"name": "workerb-retrieve-default",
	}); err != nil {
		t.Fatalf("unable to seed retrieve document: %v", err)
	}

	task := &testmodel.Task{}
	task.SetIdentifier(oid.Hex())
	if err := m.Retrieve(manipulate.NewContext(context.Background()), task); err != nil {
		t.Fatalf("unexpected retrieve error: %v", err)
	}

	if task.Name != "workerb-retrieve-default" {
		t.Fatalf("unexpected retrieved name: %q", task.Name)
	}
	if task.Status != testmodel.TaskStatusTODO {
		t.Fatalf("expected default status TODO after retrieve reset, got %q", task.Status)
	}
}

func TestWorkerBMakePreviousRetrieverMongoSuccess(t *testing.T) {
	uri, db := requireMemongo(t)

	mapi, err := New(uri, db)
	if err != nil {
		t.Fatalf("unexpected constructor error: %v", err)
	}
	m, ok := mapi.(*mongoDriverManipulator)
	if !ok {
		t.Fatalf("expected *mongoDriverManipulator, got %T", mapi)
	}

	coll := m.client.Database(m.dbName).Collection(testmodel.TaskIdentity.Name)
	oid := mongobson.NewObjectID()
	if _, err := coll.InsertOne(context.Background(), mongobson.M{
		"_id":  oid,
		"name": "workerb-previous-doc",
	}); err != nil {
		t.Fatalf("unable to seed previous retriever document: %v", err)
	}

	previousRetriever := makePreviousRetrieverMongo(coll, context.Background())
	doc, err := previousRetriever(oid)
	if err != nil {
		t.Fatalf("unexpected previous retriever error: %v", err)
	}
	if doc == nil || doc["name"] != "workerb-previous-doc" {
		t.Fatalf("unexpected previous retriever document: %#v", doc)
	}
}
