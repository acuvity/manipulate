package manipmongo

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"go.acuvity.ai/elemental"
	"go.acuvity.ai/manipulate"
	"go.acuvity.ai/manipulate/maniptest"
	mongobson "go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mongooptions "go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

type helperTestAttributeEncrypter struct{}

func (*helperTestAttributeEncrypter) EncryptString(in string) (string, error) { return in, nil }
func (*helperTestAttributeEncrypter) DecryptString(in string) (string, error) { return in, nil }

func assertPanics(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic")
		}
	}()
	fn()
}

func databaseWriteConcern(t *testing.T, db *mongo.Database) *writeconcern.WriteConcern {
	t.Helper()
	if db == nil {
		t.Fatalf("expected non-nil database")
	}

	field := reflect.ValueOf(db).Elem().FieldByName("writeConcern")
	if !field.IsValid() {
		t.Fatalf("database writeConcern field not found")
	}

	return reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Interface().(*writeconcern.WriteConcern)
}

func TestHelpersPanicForNonMongoManipulator(t *testing.T) {
	nonMongo := maniptest.NewTestManipulator()
	identity := elemental.MakeIdentity("resource", "resources")
	index := mongo.IndexModel{Keys: mongobson.D{{Key: "name", Value: 1}}}

	assertPanics(t, func() { _, _ = DoesDatabaseExist(nonMongo) })
	assertPanics(t, func() { _ = DropDatabase(nonMongo) })
	assertPanics(t, func() { _ = CreateIndex(nonMongo, identity, index) })
	assertPanics(t, func() { _ = EnsureIndex(nonMongo, identity, index) })
	assertPanics(t, func() { _ = DeleteIndex(nonMongo, identity, "idx_name") })
	assertPanics(t, func() { _ = CreateCollection(nonMongo, identity, mongooptions.CreateCollection()) })
	assertPanics(t, func() { _, _ = GetDatabase(nonMongo) })
	assertPanics(t, func() { _ = Disconnect(nonMongo, context.Background()) })
	assertPanics(t, func() {
		_ = SetConsistencyMode(nonMongo, manipulate.ReadConsistencyStrong, manipulate.WriteConsistencyStrong)
	})
	assertPanics(t, func() { SetAttributeEncrypter(nonMongo, &helperTestAttributeEncrypter{}) })
	assertPanics(t, func() { _ = GetAttributeEncrypter(nonMongo) })
}

func TestAttributeEncrypterRoundTrip(t *testing.T) {
	enc := &helperTestAttributeEncrypter{}
	m := &mongoManipulator{}

	SetAttributeEncrypter(m, enc)
	if got := GetAttributeEncrypter(m); got != enc {
		t.Fatalf("unexpected attribute encrypter: got %T want %T", got, enc)
	}
}

func TestGetDatabaseAndSetConsistencyMode(t *testing.T) {
	uri, dbName := requireMemongo(t)
	api, err := New(uri, dbName, OptionForceReadFilter(mongobson.D{}))
	if err != nil {
		t.Fatalf("unable to create mongo manipulator: %v", err)
	}

	db, err := GetDatabase(api)
	if err != nil {
		t.Fatalf("GetDatabase returned unexpected error: %v", err)
	}
	if db == nil || db.Name() != dbName {
		t.Fatalf("unexpected database: %#v", db)
	}

	if err := SetConsistencyMode(api, manipulate.ReadConsistencyNearest, manipulate.WriteConsistencyNone); err != nil {
		t.Fatalf("SetConsistencyMode returned unexpected error: %v", err)
	}

	m, ok := api.(*mongoManipulator)
	if !ok {
		t.Fatalf("expected *mongoManipulator, got %T", api)
	}
	readConsistency, writeConsistency := m.defaultConsistency()
	if readConsistency != manipulate.ReadConsistencyNearest || writeConsistency != manipulate.WriteConsistencyNone {
		t.Fatalf("unexpected default consistency: got (%v, %v)", readConsistency, writeConsistency)
	}

	db, err = GetDatabase(api)
	if err != nil {
		t.Fatalf("GetDatabase after SetConsistencyMode returned unexpected error: %v", err)
	}
	if wc := databaseWriteConcern(t, db); wc == nil || wc.Acknowledged() {
		t.Fatalf("expected GetDatabase handle to reflect unacknowledged default writes, got %#v", wc)
	}

	if wc := databaseWriteConcern(t, m.makeAcknowledgedDatabase()); wc == nil || !wc.Acknowledged() {
		t.Fatalf("expected helper database to force acknowledged writes, got %#v", wc)
	}
}

func TestSetConsistencyModeDefaultLeavesExistingDefaultsUnchanged(t *testing.T) {
	uri, dbName := requireMemongo(t)
	api, err := New(
		uri,
		dbName,
		OptionForceReadFilter(mongobson.D{}),
		OptionDefaultReadConsistencyMode(manipulate.ReadConsistencyStrong),
		OptionDefaultWriteConsistencyMode(manipulate.WriteConsistencyStrong),
	)
	if err != nil {
		t.Fatalf("unable to create mongo manipulator: %v", err)
	}

	if err := SetConsistencyMode(api, manipulate.ReadConsistencyMonotonic, manipulate.WriteConsistencyDefault); err != nil {
		t.Fatalf("SetConsistencyMode returned unexpected error: %v", err)
	}

	m, ok := api.(*mongoManipulator)
	if !ok {
		t.Fatalf("expected *mongoManipulator, got %T", api)
	}

	readConsistency, writeConsistency := m.defaultConsistency()
	if readConsistency != manipulate.ReadConsistencyMonotonic || writeConsistency != manipulate.WriteConsistencyStrong {
		t.Fatalf("unexpected default consistency after preserving write default: got (%v, %v)", readConsistency, writeConsistency)
	}

	db, err := GetDatabase(api)
	if err != nil {
		t.Fatalf("GetDatabase returned unexpected error: %v", err)
	}
	if wc := databaseWriteConcern(t, db); wc == nil || !wc.Acknowledged() || wc.W != writeconcern.WCMajority {
		t.Fatalf("expected GetDatabase handle to preserve majority write concern, got %#v", wc)
	}
}

func TestDisconnect(t *testing.T) {
	originalDisconnect := mongoDisconnectFn
	defer func() { mongoDisconnectFn = originalDisconnect }()

	t.Run("uses provided context", func(t *testing.T) {
		type ctxKey string
		const key ctxKey = "k"
		wantErr := context.Canceled
		seen := false

		mongoDisconnectFn = func(client *mongo.Client, ctx context.Context) error {
			seen = true
			if client == nil {
				t.Fatalf("expected non-nil client")
			}
			if got := ctx.Value(key); got != "v" {
				t.Fatalf("unexpected context value: got %#v", got)
			}
			return wantErr
		}

		m := &mongoManipulator{client: &mongo.Client{}, operationTimeout: time.Second}
		err := Disconnect(m, context.WithValue(context.Background(), key, "v"))
		if !errors.Is(err, wantErr) {
			t.Fatalf("unexpected disconnect error: got %v want %v", err, wantErr)
		}
		if !seen {
			t.Fatalf("expected disconnect function to be called")
		}
	})

	t.Run("nil context uses default timeout", func(t *testing.T) {
		seen := false
		mongoDisconnectFn = func(client *mongo.Client, ctx context.Context) error {
			seen = true
			if client == nil {
				t.Fatalf("expected non-nil client")
			}
			if err := ctx.Err(); err != nil {
				t.Fatalf("expected active disconnect context, got %v", err)
			}
			deadline, ok := ctx.Deadline()
			if !ok {
				t.Fatalf("expected default disconnect context to have a deadline")
			}
			remaining := time.Until(deadline)
			if remaining <= 0 || remaining > 2*time.Second {
				t.Fatalf("unexpected disconnect deadline remaining: %v", remaining)
			}
			return nil
		}

		m := &mongoManipulator{client: &mongo.Client{}, operationTimeout: time.Second}
		if err := Disconnect(m, nil); err != nil {
			t.Fatalf("Disconnect returned unexpected error: %v", err)
		}
		if !seen {
			t.Fatalf("expected disconnect function to be called")
		}
	})
}

func TestPrepareMongoIndexModelDoesNotMutateCallerOptions(t *testing.T) {
	identity := elemental.MakeIdentity("resource", "resources")
	originalOptions := mongooptions.Index().SetUnique(true)
	originalModel := mongo.IndexModel{
		Keys:    mongobson.D{{Key: "name", Value: 1}},
		Options: originalOptions,
	}

	prepared0, idxName0, _, _, err := prepareMongoIndexModel(identity, 0, originalModel)
	if err != nil {
		t.Fatalf("prepareMongoIndexModel returned unexpected error: %v", err)
	}
	prepared1, idxName1, _, _, err := prepareMongoIndexModel(identity, 1, originalModel)
	if err != nil {
		t.Fatalf("prepareMongoIndexModel returned unexpected error on reuse: %v", err)
	}

	originalApplied, err := applyMongoIndexOptions(originalOptions)
	if err != nil {
		t.Fatalf("applyMongoIndexOptions on original builder returned unexpected error: %v", err)
	}
	if originalApplied.Name != nil {
		t.Fatalf("expected original index options builder to remain unnamed, got %q", *originalApplied.Name)
	}

	preparedApplied0, err := applyMongoIndexOptions(prepared0.Options)
	if err != nil {
		t.Fatalf("applyMongoIndexOptions on prepared builder 0 returned unexpected error: %v", err)
	}
	preparedApplied1, err := applyMongoIndexOptions(prepared1.Options)
	if err != nil {
		t.Fatalf("applyMongoIndexOptions on prepared builder 1 returned unexpected error: %v", err)
	}

	if preparedApplied0.Name == nil || *preparedApplied0.Name != idxName0 {
		t.Fatalf("expected prepared builder 0 to carry generated name %q, got %#v", idxName0, preparedApplied0.Name)
	}
	if preparedApplied1.Name == nil || *preparedApplied1.Name != idxName1 {
		t.Fatalf("expected prepared builder 1 to carry generated name %q, got %#v", idxName1, preparedApplied1.Name)
	}
	if idxName0 == idxName1 {
		t.Fatalf("expected distinct generated names for reused model, got %q", idxName0)
	}
}

func TestIsMongoManipulator(t *testing.T) {
	if !IsMongoManipulator(&mongoManipulator{}) {
		t.Fatalf("expected mongoManipulator to be recognized")
	}
	if IsMongoManipulator(maniptest.NewTestManipulator()) {
		t.Fatalf("expected non-mongo manipulator to be rejected")
	}
}
