package manipmongo

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.acuvity.ai/elemental"
	"go.acuvity.ai/manipulate"
	"go.acuvity.ai/manipulate/maniptest"
	bson "go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type workerAAttributeEncrypter struct{}

func (*workerAAttributeEncrypter) EncryptString(in string) (string, error) { return in, nil }
func (*workerAAttributeEncrypter) DecryptString(in string) (string, error) { return in, nil }

func workerATestMongoDriverManipulator(t *testing.T) *mongoDriverManipulator {
	t.Helper()

	client, err := mongo.Connect(
		options.Client().
			ApplyURI("mongodb://127.0.0.1:1").
			SetConnectTimeout(10 * time.Millisecond).
			SetServerSelectionTimeout(10 * time.Millisecond).
			SetTimeout(10 * time.Millisecond),
	)
	if err != nil {
		t.Fatalf("unable to initialize test mongo client: %v", err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_ = client.Disconnect(ctx)
	})

	return &mongoDriverManipulator{
		client: client,
		dbName: "helpers_worker_a",
	}
}

func TestWorkerAHelperAPIsRequireMongoDriverManipulator(t *testing.T) {
	nonMongo := maniptest.NewTestManipulator()
	identity := elemental.MakeIdentity("resource", "resources")
	index := mongo.IndexModel{Keys: bson.D{{Key: "name", Value: 1}}}

	if _, err := DoesDatabaseExist(nonMongo); !errors.Is(err, ErrMongoAPIRequiresMongoManipulator) {
		t.Fatalf("DoesDatabaseExist expected ErrMongoAPIRequiresMongoManipulator, got %v", err)
	}
	if err := DropDatabase(nonMongo); !errors.Is(err, ErrMongoAPIRequiresMongoManipulator) {
		t.Fatalf("DropDatabase expected ErrMongoAPIRequiresMongoManipulator, got %v", err)
	}
	if err := CreateIndex(nonMongo, identity, index); !errors.Is(err, ErrMongoAPIRequiresMongoManipulator) {
		t.Fatalf("CreateIndex expected ErrMongoAPIRequiresMongoManipulator, got %v", err)
	}
	if err := EnsureIndex(nonMongo, identity, index); !errors.Is(err, ErrMongoAPIRequiresMongoManipulator) {
		t.Fatalf("EnsureIndex expected ErrMongoAPIRequiresMongoManipulator, got %v", err)
	}
	if err := DeleteIndex(nonMongo, identity, "idx_name"); !errors.Is(err, ErrMongoAPIRequiresMongoManipulator) {
		t.Fatalf("DeleteIndex expected ErrMongoAPIRequiresMongoManipulator, got %v", err)
	}
	if err := CreateCollection(nonMongo, identity); !errors.Is(err, ErrMongoAPIRequiresMongoManipulator) {
		t.Fatalf("CreateCollection expected ErrMongoAPIRequiresMongoManipulator, got %v", err)
	}
	if _, err := GetDatabase(nonMongo); !errors.Is(err, ErrMongoAPIRequiresMongoManipulator) {
		t.Fatalf("GetDatabase expected ErrMongoAPIRequiresMongoManipulator, got %v", err)
	}
	if err := SetConsistencyMode(nonMongo, manipulate.ReadConsistencyStrong, manipulate.WriteConsistencyStrong); !errors.Is(err, ErrMongoAPIRequiresMongoManipulator) {
		t.Fatalf("SetConsistencyMode expected ErrMongoAPIRequiresMongoManipulator, got %v", err)
	}
	if err := SetAttributeEncrypterSafe(nonMongo, nil); !errors.Is(err, ErrMongoAPIRequiresMongoManipulator) {
		t.Fatalf("SetAttributeEncrypterSafe expected ErrMongoAPIRequiresMongoManipulator, got %v", err)
	}
	if _, err := GetAttributeEncrypterSafe(nonMongo); !errors.Is(err, ErrMongoAPIRequiresMongoManipulator) {
		t.Fatalf("GetAttributeEncrypterSafe expected ErrMongoAPIRequiresMongoManipulator, got %v", err)
	}
}

func TestWorkerASetAttributeEncrypterPanicsForNonMongoManipulator(t *testing.T) {
	nonMongo := maniptest.NewTestManipulator()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expected panic")
		}
		got, ok := r.(string)
		if !ok {
			t.Fatalf("unexpected panic type: %T", r)
		}
		if got != "you can only pass a mongo manipulator to SetAttributeEncrypter" {
			t.Fatalf("unexpected panic message: %s", got)
		}
	}()

	SetAttributeEncrypter(nonMongo, &workerAAttributeEncrypter{})
}

func TestWorkerAGetAttributeEncrypterPanicsForNonMongoManipulator(t *testing.T) {
	nonMongo := maniptest.NewTestManipulator()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expected panic")
		}
		got, ok := r.(string)
		if !ok {
			t.Fatalf("unexpected panic type: %T", r)
		}
		if got != "you can only pass a mongo manipulator to GetAttributeEncrypter" {
			t.Fatalf("unexpected panic message: %s", got)
		}
	}()

	_ = GetAttributeEncrypter(nonMongo)
}

func TestWorkerAAttributeEncrypterSetGetSuccessPaths(t *testing.T) {
	m := &mongoDriverManipulator{}
	enc := &workerAAttributeEncrypter{}

	if err := SetAttributeEncrypterSafe(m, enc); err != nil {
		t.Fatalf("SetAttributeEncrypterSafe returned unexpected error: %v", err)
	}

	got, err := GetAttributeEncrypterSafe(m)
	if err != nil {
		t.Fatalf("GetAttributeEncrypterSafe returned unexpected error: %v", err)
	}
	if got != enc {
		t.Fatalf("GetAttributeEncrypterSafe returned unexpected encrypter pointer")
	}

	SetAttributeEncrypter(m, nil)
	if out := GetAttributeEncrypter(m); out != nil {
		t.Fatalf("GetAttributeEncrypter expected nil after SetAttributeEncrypter(nil), got %T", out)
	}
}

func TestWorkerADeleteIndexEmptyIndexesReturnsNil(t *testing.T) {
	m := workerATestMongoDriverManipulator(t)
	identity := elemental.MakeIdentity("resource", "resources")

	if err := DeleteIndex(m, identity); err != nil {
		t.Fatalf("DeleteIndex with empty index list should return nil, got: %v", err)
	}
}

func TestWorkerADeleteIndexReturnsOperationError(t *testing.T) {
	m := workerATestMongoDriverManipulator(t)
	identity := elemental.MakeIdentity("resource", "resources")

	if err := DeleteIndex(m, identity, "idx_name"); err == nil {
		t.Fatalf("DeleteIndex expected operation error for unreachable mongo")
	}
}

func TestWorkerACreateIndexesWithContextBranches(t *testing.T) {
	identity := elemental.MakeIdentity("resource", "resources")
	index := mongo.IndexModel{Keys: bson.D{{Key: "name", Value: 1}}}

	if err := createIndexesWithContext(context.Background(), &mongoDriverManipulator{}, identity); err != nil {
		t.Fatalf("createIndexesWithContext with empty indexes should return nil, got: %v", err)
	}

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := createIndexesWithContext(cancelCtx, &mongoDriverManipulator{}, identity, index); !errors.Is(err, context.Canceled) {
		t.Fatalf("createIndexesWithContext with canceled context expected context.Canceled, got: %v", err)
	}

	m := workerATestMongoDriverManipulator(t)
	err := createIndexesWithContext(nil, m, identity, index)
	if err == nil {
		t.Fatalf("createIndexesWithContext with nil context and unreachable mongo should fail")
	}
	if !strings.Contains(err.Error(), "unable to create indexes") {
		t.Fatalf("unexpected createIndexesWithContext nil-context error: %v", err)
	}
}

func TestWorkerAEnsureIndexesWithContextBranches(t *testing.T) {
	identity := elemental.MakeIdentity("resource", "resources")
	index := mongo.IndexModel{
		Keys:    bson.D{{Key: "name", Value: 1}},
		Options: options.Index().SetName("idx_name"),
	}

	if err := ensureIndexesWithContext(context.Background(), &mongoDriverManipulator{}, identity); err != nil {
		t.Fatalf("ensureIndexesWithContext with empty indexes should return nil, got: %v", err)
	}

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := ensureIndexesWithContext(cancelCtx, &mongoDriverManipulator{}, identity, index); !errors.Is(err, context.Canceled) {
		t.Fatalf("ensureIndexesWithContext with canceled context expected context.Canceled, got: %v", err)
	}

	m := workerATestMongoDriverManipulator(t)
	err := ensureIndexesWithContext(nil, m, identity, index)
	if err == nil {
		t.Fatalf("ensureIndexesWithContext with nil context and unreachable mongo should fail")
	}
	if !strings.Contains(err.Error(), "unable to ensure index") {
		t.Fatalf("unexpected ensureIndexesWithContext nil-context error: %v", err)
	}
}

func TestWorkerACreateCollectionWithContextBranches(t *testing.T) {
	identity := elemental.MakeIdentity("resource", "resources")

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := createCollectionWithContext(cancelCtx, &mongoDriverManipulator{}, identity); !errors.Is(err, context.Canceled) {
		t.Fatalf("createCollectionWithContext with canceled context expected context.Canceled, got: %v", err)
	}

	m := workerATestMongoDriverManipulator(t)
	err := createCollectionWithContext(nil, m, identity)
	if err == nil {
		t.Fatalf("createCollectionWithContext with nil context and unreachable mongo should fail")
	}
	if !strings.Contains(err.Error(), "unable to create collection") {
		t.Fatalf("unexpected createCollectionWithContext nil-context error: %v", err)
	}
}

func TestWorkerARunQueryRetriesOnCommunicationError(t *testing.T) {
	attempts := 0
	retryCalls := 0

	mctx := manipulate.NewContext(context.Background(), manipulate.ContextOptionRetryFunc(func(manipulate.RetryInfo) error {
		retryCalls++
		return nil
	}))

	out, err := RunQuery(
		mctx,
		func() (any, error) {
			attempts++
			if attempts == 1 {
				return nil, mongo.CommandError{Code: 6, Message: "temporary communication failure"}
			}
			return "ok", nil
		},
		RetryInfo{Identity: elemental.MakeIdentity("resource", "resources")},
	)
	if err != nil {
		t.Fatalf("RunQuery returned unexpected error: %v", err)
	}
	if out != "ok" {
		t.Fatalf("unexpected RunQuery output: %v", out)
	}
	if attempts != 2 || retryCalls != 1 {
		t.Fatalf("unexpected retry behavior: attempts=%d retryCalls=%d", attempts, retryCalls)
	}
}

func TestWorkerARunQueryReturnsNonCommunicationErrorWithoutRetry(t *testing.T) {
	expectedErr := errors.New("not-a-communication-error")

	out, err := RunQuery(
		manipulate.NewContext(context.Background()),
		func() (any, error) {
			return "partial", expectedErr
		},
		RetryInfo{Identity: elemental.MakeIdentity("resource", "resources")},
	)
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected original non-communication error, got: %v", err)
	}
	if out != "partial" {
		t.Fatalf("expected operation output to be preserved, got: %v", out)
	}
}

func TestWorkerARunQueryUsesDefaultRetryFunc(t *testing.T) {
	attempts := 0
	defaultRetryCalls := 0

	out, err := RunQuery(
		manipulate.NewContext(context.Background()),
		func() (any, error) {
			attempts++
			if attempts == 1 {
				return nil, mongo.CommandError{Code: 6, Message: "temporary communication failure"}
			}
			return "ok-default", nil
		},
		RetryInfo{
			Identity: elemental.MakeIdentity("resource", "resources"),
			defaultRetryFunc: func(manipulate.RetryInfo) error {
				defaultRetryCalls++
				return nil
			},
		},
	)
	if err != nil {
		t.Fatalf("RunQuery returned unexpected error: %v", err)
	}
	if out != "ok-default" {
		t.Fatalf("unexpected RunQuery output: %v", out)
	}
	if attempts != 2 || defaultRetryCalls != 1 {
		t.Fatalf("unexpected default retry behavior: attempts=%d defaultRetryCalls=%d", attempts, defaultRetryCalls)
	}
}

func TestWorkerARunQueryReturnsRetryFuncError(t *testing.T) {
	retryErr := errors.New("retry function failed")
	mctx := manipulate.NewContext(context.Background(), manipulate.ContextOptionRetryFunc(func(manipulate.RetryInfo) error {
		return retryErr
	}))

	_, err := RunQuery(
		mctx,
		func() (any, error) {
			return nil, mongo.CommandError{Code: 6, Message: "temporary communication failure"}
		},
		RetryInfo{Identity: elemental.MakeIdentity("resource", "resources")},
	)
	if !errors.Is(err, retryErr) {
		t.Fatalf("expected retry function error, got: %v", err)
	}
}

func TestWorkerARunQueryReturnsDefaultRetryFuncError(t *testing.T) {
	retryErr := errors.New("default retry function failed")

	_, err := RunQuery(
		manipulate.NewContext(context.Background()),
		func() (any, error) {
			return nil, mongo.CommandError{Code: 6, Message: "temporary communication failure"}
		},
		RetryInfo{
			Identity: elemental.MakeIdentity("resource", "resources"),
			defaultRetryFunc: func(manipulate.RetryInfo) error {
				return retryErr
			},
		},
	)
	if !errors.Is(err, retryErr) {
		t.Fatalf("expected default retry function error, got: %v", err)
	}
}

func TestWorkerARunQueryReturnsContextErrorWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	mctx := manipulate.NewContext(ctx)

	_, err := RunQuery(
		mctx,
		func() (any, error) {
			return nil, mongo.CommandError{Code: 6, Message: "still unreachable"}
		},
		RetryInfo{Identity: elemental.MakeIdentity("resource", "resources")},
	)
	if err == nil {
		t.Fatalf("expected context cancellation error")
	}

	var cannotExecute manipulate.ErrCannotExecuteQuery
	if !errors.As(err, &cannotExecute) {
		t.Fatalf("expected manipulate.ErrCannotExecuteQuery, got %T (%v)", err, err)
	}
}

func TestWorkerAGetDatabaseAndSetConsistencyModeSuccessPaths(t *testing.T) {
	m := workerATestMongoDriverManipulator(t)

	db, err := GetDatabase(m)
	if err != nil {
		t.Fatalf("GetDatabase returned unexpected error: %v", err)
	}
	if db == nil || db.Name() != m.dbName {
		t.Fatalf("GetDatabase returned unexpected database: %#v", db)
	}

	if err := SetConsistencyMode(m, manipulate.ReadConsistencyMonotonic, manipulate.WriteConsistencyStrong); err != nil {
		t.Fatalf("SetConsistencyMode returned unexpected error: %v", err)
	}
	if m.defaultReadConsistency != manipulate.ReadConsistencyMonotonic {
		t.Fatalf("default read consistency not updated: %v", m.defaultReadConsistency)
	}
	if m.defaultWriteConsistency != manipulate.WriteConsistencyStrong {
		t.Fatalf("default write consistency not updated: %v", m.defaultWriteConsistency)
	}
}

func TestWorkerAMongoManipulatorTypeHelpers(t *testing.T) {
	driver := &mongoDriverManipulator{}
	nonMongo := maniptest.NewTestManipulator()

	if !IsMongoManipulator(driver) {
		t.Fatalf("IsMongoManipulator must be true for official-driver manipulator")
	}
	if IsMongoManipulator(nonMongo) {
		t.Fatalf("IsMongoManipulator must be false for non-mongo manipulator")
	}

	upsertCtx := manipulate.NewContext(context.Background(), contextOptionUpsertCanonical(bson.M{"$setOnInsert": bson.M{"name": "value"}}))
	if !IsUpsert(upsertCtx) {
		t.Fatalf("expected upsert context to be detected")
	}
	if IsUpsert(manipulate.NewContext(context.Background())) {
		t.Fatalf("expected regular context to not be detected as upsert")
	}
}
