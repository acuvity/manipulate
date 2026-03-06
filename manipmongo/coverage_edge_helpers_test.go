package manipmongo

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/tryvium-travels/memongo"
	"github.com/tryvium-travels/memongo/memongolog"
	"go.acuvity.ai/elemental"
	mongobson "go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mongooptions "go.mongodb.org/mongo-driver/v2/mongo/options"
)

type coverageExplainable struct {
	result any
	err    error
}

func (c coverageExplainable) Explain(result interface{}) error {
	if c.err != nil {
		return c.err
	}
	if out, ok := result.(*mongobson.M); ok {
		(*out)["result"] = c.result
	}
	return nil
}

func TestCoverageIndexOptionAndNormalizeErrorBranches(t *testing.T) {
	if got, err := applyMongoIndexOptions(nil); err != nil || got == nil {
		t.Fatalf("expected nil builder to produce default options, got=%v err=%v", got, err)
	}

	boom := errors.New("apply-failed")
	badBuilder := &mongooptions.IndexOptionsBuilder{
		Opts: []func(*mongooptions.IndexOptions) error{
			func(*mongooptions.IndexOptions) error { return boom },
		},
	}
	if _, err := applyMongoIndexOptions(badBuilder); !errors.Is(err, boom) {
		t.Fatalf("expected applyMongoIndexOptions error branch, got %v", err)
	}

	uri, db := requireMemongo(t)
	mongoAPI, err := New(uri, db, optionForceReadFilterCanonical(mongobson.D{}))
	if err != nil {
		t.Fatalf("unable to create mongo manipulator: %v", err)
	}

	identity := elemental.MakeIdentity("mongoindexbranch", "mongoindexbranch")
	err = EnsureIndex(
		mongoAPI,
		identity,
		mongo.IndexModel{
			Keys:    mongobson.D{{Key: "name", Value: 1}},
			Options: badBuilder,
		},
	)
	if err == nil || !strings.Contains(err.Error(), "unable to prepare index") {
		t.Fatalf("expected normalize/apply index options error, got %v", err)
	}

	if isMongoIndexConflictError(nil) {
		t.Fatalf("expected nil error to not be a conflict")
	}
}

func TestCoverageDoesDatabaseExistConnectionErrorBranch(t *testing.T) {
	opts := &memongo.Options{
		MongoVersion:   "4.4.26",
		StartupTimeout: 45 * time.Second,
		LogLevel:       memongolog.LogLevelWarn,
	}
	if runtime.GOOS == "darwin" && runtime.GOARCH == "arm64" {
		opts.DownloadURL = "https://fastdl.mongodb.org/osx/mongodb-macos-x86_64-4.4.26.tgz"
	}

	server, err := memongo.StartWithOptions(opts)
	if err != nil {
		skipOrFailMemongo(t, "unable to start dedicated memongo for connection-error branch: %v", err)
	}
	defer server.Stop()

	api, err := New(server.URI(), dbNameForTest(t))
	if err != nil {
		t.Fatalf("unable to create manipulator for connection-error branch: %v", err)
	}

	server.Stop()
	if _, err := DoesDatabaseExist(api); err == nil {
		t.Fatalf("expected DoesDatabaseExist connection error after server stop")
	}
}

func TestCoverageEnsureIndexesMongoConflictErrorBranches(t *testing.T) {
	uri, db := requireMemongo(t)
	mongoAPI, err := New(uri, db, optionForceReadFilterCanonical(mongobson.D{}))
	if err != nil {
		t.Fatalf("unable to create mongo manipulator: %v", err)
	}

	identity := elemental.MakeIdentity("ensureidxconflict", "ensureidxconflict")
	if err := CreateCollection(mongoAPI, identity); err != nil {
		t.Fatalf("unable to create mongo collection for conflict branches: %v", err)
	}

	dbObj, closeDB, err := getDatabaseCanonical(mongoAPI)
	if err != nil {
		t.Fatalf("unable to get mongo database: %v", err)
	}
	defer closeDB()
	coll := dbObj.Collection(identity.Name)

	if _, err := coll.InsertMany(context.Background(), []any{
		mongobson.M{"a": 1},
		mongobson.M{"a": 1},
	}); err != nil {
		t.Fatalf("unable to seed duplicate docs for unique-index recreate error branch: %v", err)
	}

	if err := CreateIndex(
		mongoAPI,
		identity,
		mongo.IndexModel{
			Keys:    mongobson.D{{Key: "a", Value: 1}},
			Options: mongooptions.Index().SetName("idx_a"),
		},
	); err != nil {
		t.Fatalf("unable to create baseline index for recreate-error branch: %v", err)
	}

	if err := EnsureIndex(
		mongoAPI,
		identity,
		mongo.IndexModel{
			Keys:    mongobson.D{{Key: "a", Value: 1}},
			Options: mongooptions.Index().SetName("idx_a").SetUnique(true),
		},
	); err == nil || !strings.Contains(err.Error(), "unable to ensure index after dropping old one 'idx_a'") {
		t.Fatalf("expected recreate-after-drop error branch, got %v", err)
	}

	if err := CreateIndex(
		mongoAPI,
		identity,
		mongo.IndexModel{
			Keys:    mongobson.D{{Key: "a", Value: 1}},
			Options: mongooptions.Index().SetName("idx_ttl"),
		},
	); err != nil {
		t.Fatalf("unable to create baseline non-ttl index for ttl-update-error branch: %v", err)
	}

	if err := EnsureIndex(
		mongoAPI,
		identity,
		mongo.IndexModel{
			Keys:    mongobson.D{{Key: "a", Value: 1}},
			Options: mongooptions.Index().SetName("idx_ttl").SetExpireAfterSeconds(10),
		},
	); err == nil || !strings.Contains(err.Error(), "cannot update TTL index 'idx_ttl'") {
		t.Fatalf("expected TTL update error branch, got %v", err)
	}
}

func TestCoverageExplainBranches(t *testing.T) {
	identity := elemental.MakeIdentity("edgeexplain", "edgeexplains")
	opMap := map[elemental.Identity]map[elemental.Operation]struct{}{
		identity: nil,
	}

	explainFunc := explainIfNeeded(
		coverageExplainable{result: "ok"},
		mongobson.D{{Key: "name", Value: "value"}},
		identity,
		elemental.OperationRetrieve,
		opMap,
	)
	if explainFunc == nil {
		t.Fatalf("expected explain callback when identity is mapped to nil operations map")
	}
	if err := explainFunc(); err != nil {
		t.Fatalf("expected explain callback success, got %v", err)
	}

	notMatched := explainIfNeeded(
		coverageExplainable{result: "ok"},
		mongobson.D{{Key: "name", Value: "value"}},
		identity,
		elemental.OperationRetrieve,
		map[elemental.Identity]map[elemental.Operation]struct{}{
			identity: {elemental.OperationCreate: {}},
		},
	)
	if notMatched != nil {
		t.Fatalf("expected nil callback when operation does not match")
	}

	if err := explain(
		coverageExplainable{result: "ok"},
		elemental.OperationRetrieve,
		identity,
		mongobson.D{{Key: "bad", Value: func() {}}},
	); err == nil || !strings.Contains(err.Error(), "unable to marshal filter") {
		t.Fatalf("expected filter marshal error, got %v", err)
	}

	if err := explain(
		coverageExplainable{result: func() {}},
		elemental.OperationRetrieve,
		identity,
		mongobson.D{{Key: "name", Value: "value"}},
	); err == nil || !strings.Contains(err.Error(), "unable to marshal explanation") {
		t.Fatalf("expected explanation marshal error, got %v", err)
	}
}

func TestCoverageMakePreviousRetrieverMongoNotFound(t *testing.T) {
	uri, db := requireMemongo(t)
	mongoAPI, err := New(uri, db, optionForceReadFilterCanonical(mongobson.D{}))
	if err != nil {
		t.Fatalf("unable to create mongo manipulator: %v", err)
	}

	internal := mongoAPI.(*mongoDriverManipulator)
	coll := internal.client.Database(internal.dbName).Collection(mongoIntegrationIdentity.Name)
	prev := makePreviousRetrieverMongo(coll, context.Background())
	if _, err := prev(mongobson.NewObjectID()); err == nil {
		t.Fatalf("expected missing object error")
	}
}

func TestCoverageEnsureIndexesMongoDropErrorBranch(t *testing.T) {
	uri, db := requireMemongo(t)
	mongoAPI, err := New(uri, db, optionForceReadFilterCanonical(mongobson.D{}))
	if err != nil {
		t.Fatalf("unable to create mongo manipulator: %v", err)
	}

	identity := elemental.MakeIdentity("mongoensureidxdrop", "mongoensureidxdrops")
	if err := CreateCollection(mongoAPI, identity); err != nil {
		t.Fatalf("unable to create mongo collection: %v", err)
	}

	err = EnsureIndex(
		mongoAPI,
		identity,
		mongo.IndexModel{
			Keys:    mongobson.D{{Key: "name", Value: 1}},
			Options: mongooptions.Index().SetName("_id_"),
		},
	)
	if err == nil || !strings.Contains(err.Error(), "cannot delete previous index '_id_'") {
		t.Fatalf("expected mongo ensure-index drop error branch, got %v", err)
	}
}

func TestCoverageCountFromResultsBranches(t *testing.T) {
	if got, err := countFromResults(nil); err != nil || got != 0 {
		t.Fatalf("expected empty count results to return 0,nil; got=%d err=%v", got, err)
	}
	if got, err := countFromResults([]*countRes{{Count: 11}}); err != nil || got != 11 {
		t.Fatalf("expected single count result to return 11,nil; got=%d err=%v", got, err)
	}
	if _, err := countFromResults([]*countRes{{Count: 1}, {Count: 2}}); err == nil || !strings.Contains(err.Error(), "invalid count result len: 2") {
		t.Fatalf("expected invalid-length count result error branch, got %v", err)
	}
}
