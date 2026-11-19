package manipmongo

import (
	"bytes"
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	legacybson "github.com/globalsign/mgo/bson"
	"go.acuvity.ai/elemental"
	testmodel "go.acuvity.ai/elemental/test/model"
	"go.acuvity.ai/manipulate"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var conditionalIdentity = elemental.MakeIdentity("conditionalitem", "conditionalitems")

type conditionalObject struct {
	ID           string     `bson:"-"`
	ModelVersion int        `bson:"-"`
	Namespace    *string    `bson:"namespace,omitempty"`
	Revision     *int64     `bson:"rev,omitempty"`
	Name         *string    `bson:"name,omitempty"`
	Optional     *string    `bson:"opt,omitempty"`
	FirstSeen    *time.Time `bson:"first,omitempty"`
	LastSeen     *time.Time `bson:"last,omitempty"`
	Secret       *string    `bson:"secret,omitempty"`
	Zone         *int64     `bson:"zone,omitempty"`
	ZHash        *int64     `bson:"zhash,omitempty"`
	MinRevision  float64    `bson:"-"`
	MaxRevision  float64    `bson:"-"`
}

type conditionalStored conditionalObject

func (*conditionalObject) Identity() elemental.Identity           { return conditionalIdentity }
func (o *conditionalObject) Identifier() string                   { return o.ID }
func (o *conditionalObject) SetIdentifier(id string)              { o.ID = id }
func (o *conditionalObject) Version() int                         { return o.ModelVersion }
func (o *conditionalObject) ToPlain() elemental.PlainIdentifiable { return conditionalPlain{o} }
func (*conditionalObject) ValueForAttribute(string) any           { return nil }

type conditionalPlain struct{ *conditionalObject }

func (p conditionalPlain) ToSparse(...string) elemental.SparseIdentifiable {
	return p.conditionalObject
}

func (o conditionalPlain) AttributeSpecifications() map[string]elemental.AttributeSpecification {
	out := map[string]elemental.AttributeSpecification{}
	for _, field := range []struct{ name, key, kind string }{
		{"namespace", "namespace", "string"}, {"revision", "rev", "integer"},
		{"name", "name", "string"}, {"optional", "opt", "string"},
		{"firstSeen", "first", "time"}, {"lastSeen", "last", "time"}, {"secret", "secret", "string"},
		{"zone", "zone", "integer"}, {"zHash", "zhash", "integer"},
	} {
		out[field.name] = elemental.AttributeSpecification{
			Name: field.name, ConvertedName: strings.ToUpper(field.name[:1]) + field.name[1:],
			BSONFieldName: field.key, Type: field.kind, Stored: true,
			CreationOnly: field.name == "firstSeen", Encrypted: field.name == "secret", Required: field.name == "name",
		}
	}
	bounds := out["revision"]
	bounds.MinValue, bounds.MaxValue = o.MinRevision, o.MaxRevision
	out["revision"] = bounds
	return out
}

func (o conditionalPlain) SpecificationForAttribute(name string) elemental.AttributeSpecification {
	return o.AttributeSpecifications()[name]
}

func (o *conditionalObject) GetBSON() (any, error) {
	var id legacybson.ObjectId
	if o.ID != "" {
		id = legacybson.ObjectIdHex(o.ID)
	}
	return struct {
		ID                legacybson.ObjectId `bson:"_id,omitempty"`
		conditionalStored `bson:",inline"`
	}{ID: id, conditionalStored: conditionalStored(*o)}, nil
}

func (o *conditionalObject) EncryptAttributes(enc elemental.AttributeEncrypter) error {
	if o.Secret == nil {
		return nil
	}
	var err error
	*o.Secret, err = enc.EncryptString(*o.Secret)
	return err
}

func (o *conditionalObject) DecryptAttributes(enc elemental.AttributeEncrypter) error {
	if o.Secret == nil {
		return nil
	}
	var err error
	*o.Secret, err = enc.DecryptString(*o.Secret)
	return err
}

func conditionalPointer[T any](value T) *T { return &value }

func conditionalFixture(t *testing.T, opts ...Option) (*mongoManipulator, string, *mongo.Collection) {
	t.Helper()
	uri, db := requireMemongo(t)
	m, err := New(uri, db, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Disconnect(m, t.Context()) })
	id := bson.NewObjectID()
	internal := m.(*mongoManipulator)
	collection := internal.makeCollection(conditionalIdentity, manipulate.ReadConsistencyDefault, manipulate.WriteConsistencyDefault)
	if _, err := collection.InsertOne(t.Context(), bson.M{
		"_id": id, "namespace": "/owned", "rev": int64(7),
		"name": "before", "opt": "keep-me", "first": time.Unix(1000, 0).UTC(), "last": time.Unix(2000, 0).UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	return internal, id.Hex(), collection
}

func conditionalSpec(revision int64) manipulate.ConditionalUpdate {
	return manipulate.ConditionalUpdate{RevisionField: "revision", ExpectedRevision: revision}
}

func conditionalContext(t *testing.T, spec manipulate.ConditionalUpdate, opts ...manipulate.ContextOption) manipulate.Context {
	t.Helper()
	return manipulate.NewContext(t.Context(),
		manipulate.ContextOptionNamespace("/owned"),
		manipulate.ContextOptionConditionalUpdate(spec),
	).Derive(opts...)
}

func watchConditional(t *testing.T, m *mongoManipulator, onStart func(), clientOptions ...*options.ClientOptions) <-chan bson.Raw {
	t.Helper()
	uri, _ := requireMemongo(t)
	commands := make(chan bson.Raw, 16)
	opts := options.Client().ApplyURI(uri).SetAppName("conditional-" + m.dbName).SetMonitor(&event.CommandMonitor{
		Started: func(_ context.Context, event *event.CommandStartedEvent) {
			if event.DatabaseName != m.dbName {
				return
			}
			select {
			case commands <- append(bson.Raw(nil), event.Command...):
			default:
				t.Error("unexpected additional database command")
			}
			if onStart != nil && event.CommandName == "update" {
				onStart()
			}
		},
	})
	client, err := mongo.Connect(append([]*options.ClientOptions{opts}, clientOptions...)...)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Ping(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	previous := m.client
	m.client = client
	t.Cleanup(func() { _ = previous.Disconnect(context.Background()); _ = client.Disconnect(context.Background()) })
	return commands
}

func requireConditionalCommand(t *testing.T, commands <-chan bson.Raw) bson.Raw {
	t.Helper()
	if len(commands) != 1 {
		t.Fatalf("want one atomic write and no pre-read, got %d database commands", len(commands))
	}
	command := <-commands
	if _, ok := command.Lookup("update").StringValueOK(); !ok {
		t.Fatalf("expected a write without a document result: %v", command)
	}
	return command
}

func readConditional(t *testing.T, collection *mongo.Collection, id string) bson.M {
	t.Helper()
	var row bson.M
	oid, _ := bson.ObjectIDFromHex(id)
	if err := collection.FindOne(t.Context(), bson.M{"_id": oid}).Decode(&row); err != nil {
		t.Fatal(err)
	}
	return row
}

func conditionalFailpoint(t *testing.T, m *mongoManipulator, data bson.M) {
	t.Helper()
	if !mongoEndpoint.allowFaults {
		t.Skipf("fault injection requires a suite-owned MongoDB or explicit %s=1 with an exclusively owned %s started with enableTestCommands=1",
			mongoTestOwnedFaultsEnv, mongoTestURIEnv)
	}
	data["appName"] = "conditional-" + m.dbName
	command := bson.D{{Key: "configureFailPoint", Value: "failCommand"}, {Key: "mode", Value: bson.M{"times": 1}}, {Key: "data", Value: data}}
	if err := m.client.Database("admin").RunCommand(t.Context(), command).Err(); err != nil {
		if requireMemongoStrictMode() {
			t.Fatalf("owned Mongo needs enableTestCommands=1: %v", err)
		}
		t.Skipf("Mongo failpoints unavailable: %v", err)
	}
	t.Cleanup(func() {
		_ = m.client.Database("admin").RunCommand(context.Background(), bson.D{{Key: "configureFailPoint", Value: "failCommand"}, {Key: "mode", Value: "off"}}).Err()
	})
}

func TestConditionalUpdateWriteConcernAndUncertainty(t *testing.T) {
	m, id, collection := conditionalFixture(t, OptionDefaultWriteConsistencyMode(manipulate.WriteConsistencyStrong))
	commands := watchConditional(t, m, nil)
	if err := m.Update(conditionalContext(t, conditionalSpec(7)), &conditionalObject{ID: id}); err != nil {
		t.Fatal(err)
	}
	command := requireConditionalCommand(t, commands)
	if command.Lookup("writeConcern").Document().Lookup("w").StringValue() != "majority" {
		t.Fatal("configured majority concern was lost")
	}
	patch := &conditionalObject{ID: id, Name: conditionalPointer("applied")}
	input := *patch
	conditionalFailpoint(t, m, bson.M{"failCommands": bson.A{"update"}, "writeConcernError": bson.M{"code": 64, "errmsg": "owned uncertainty"}})
	ctx := conditionalContext(t, conditionalSpec(8))
	err := m.Update(ctx, patch)
	if err == nil || *patch != input {
		t.Fatalf("uncertain write reported success or changed input: %v", err)
	}
	requireConditionalCommand(t, commands)
	if stored := readConditional(t, collection, id); stored["rev"] != int64(9) || stored["name"] != "applied" {
		t.Fatalf("acknowledgement error did not retain committed state: %+v", stored)
	}
	if err := m.Update(ctx, patch); !manipulate.IsPreconditionFailedError(err) {
		t.Fatalf("uncertain old revision was accepted: %v", err)
	}
	requireConditionalCommand(t, commands)
}

func TestConditionalUpdateStrongerConcernAndOperationBound(t *testing.T) {
	m, id, _ := conditionalFixture(t)
	commands := watchConditional(t, m, nil)
	ctx := conditionalContext(t, conditionalSpec(7), manipulate.ContextOptionWriteConsistency(manipulate.WriteConsistencyStrongest))
	if err := m.Update(ctx, &conditionalObject{ID: id}); err != nil {
		t.Fatal(err)
	}
	wc := requireConditionalCommand(t, commands).Lookup("writeConcern").Document()
	if wc.Lookup("w").StringValue() != "majority" || !wc.Lookup("j").Boolean() {
		t.Fatal("context majority+journal requirements were weakened")
	}
	m.operationTimeout = 20 * time.Millisecond
	conditionalFailpoint(t, m, bson.M{"failCommands": bson.A{"update"}, "blockConnection": true, "blockTimeMS": 200})
	parent, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	patch := &conditionalObject{ID: id}
	input := *patch
	err := m.Update(manipulate.NewContext(parent,
		manipulate.ContextOptionNamespace("/owned"),
		manipulate.ContextOptionConditionalUpdate(conditionalSpec(8)),
	), patch)
	if !errors.Is(err, context.DeadlineExceeded) || parent.Err() != nil || *patch != input {
		t.Fatalf("operation bound did not limit the write: %v", err)
	}
	requireConditionalCommand(t, commands)
}

func TestConditionalUpdateDoesNotReplayConnectionFailure(t *testing.T) {
	m, id, _ := conditionalFixture(t)
	commands := watchConditional(t, m, nil, options.Client().SetRetryWrites(false))
	conditionalFailpoint(t, m, bson.M{"failCommands": bson.A{"update"}, "closeConnection": true})
	patch := &conditionalObject{ID: id, Name: conditionalPointer("next")}
	input := *patch
	called := false
	ctx := conditionalContext(t, conditionalSpec(7), manipulate.ContextOptionRetryFunc(func(manipulate.RetryInfo) error {
		called = true
		return errors.New("unexpected retry callback")
	}))
	err := m.Update(ctx, patch)
	if !manipulate.IsCannotCommunicateError(err) || called || *patch != input {
		t.Fatalf("failed write reported %v, callback called: %t, patch: %+v", err, called, patch)
	}
	requireConditionalCommand(t, commands)
}

func TestConditionalUpdateRetriesSameLogicalWrite(t *testing.T) {
	m, id, collection := conditionalFixture(t)
	commands := watchConditional(t, m, nil, options.Client().SetRetryWrites(true))
	var hello struct {
		SetName string `bson:"setName"`
		Message string `bson:"msg"`
	}
	if err := m.client.Database("admin").RunCommand(t.Context(), bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		t.Fatal(err)
	}
	conditionalFailpoint(t, m, bson.M{
		"failCommands":      bson.A{"update"},
		"writeConcernError": bson.M{"code": 64, "errmsg": "owned retryable acknowledgement failure"},
		"errorLabels":       bson.A{"RetryableWriteError"},
	})
	patch := &conditionalObject{ID: id, Name: conditionalPointer("retried")}
	input := *patch
	err := m.Update(conditionalContext(t, conditionalSpec(7)), patch)
	if *patch != input {
		t.Fatalf("driver retry changed the patch: %+v", patch)
	}
	if hello.SetName == "" && hello.Message != "isdbgrid" {
		if err == nil || manipulate.IsPreconditionFailedError(err) {
			t.Fatalf("expected acknowledgement error without retry on standalone Mongo: %v", err)
		}
		requireConditionalCommand(t, commands)
	} else {
		if err != nil || len(commands) != 2 {
			t.Fatalf("expected driver retry of the acknowledged update: %v, commands=%d", err, len(commands))
		}
		first := <-commands
		retry := requireConditionalCommand(t, commands)
		before, beforeOK := first.Lookup("txnNumber").Int64OK()
		after, afterOK := retry.Lookup("txnNumber").Int64OK()
		session, sessionOK := first.Lookup("lsid").DocumentOK()
		if !beforeOK || !afterOK || before != after || !sessionOK || !bytes.Equal(session, retry.Lookup("lsid").Value) {
			t.Fatal("driver retry did not reuse the Mongo write identity")
		}
	}
	if row := readConditional(t, collection, id); row["rev"] != int64(8) || row["name"] != "retried" {
		t.Fatalf("retry did not leave exactly one revision advance: %+v", row)
	}
}

func TestConditionalUpdateWriteErrorIsNotAMiss(t *testing.T) {
	m, id, collection := conditionalFixture(t)
	if _, err := collection.Indexes().CreateOne(t.Context(), mongo.IndexModel{
		Keys: bson.D{{Key: "name", Value: 1}}, Options: options.Index().SetUnique(true),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.InsertOne(t.Context(), bson.M{"name": "taken"}); err != nil {
		t.Fatal(err)
	}
	patch := &conditionalObject{ID: id, Name: conditionalPointer("taken")}
	input := *patch
	if err := m.Update(conditionalContext(t, conditionalSpec(7)), patch); !manipulate.IsConstraintViolationError(err) || *patch != input {
		t.Fatalf("write error did not preserve the constraint violation and patch: %v", err)
	}
	if row := readConditional(t, collection, id); row["rev"] != int64(7) || row["name"] != "before" {
		t.Fatalf("rejected write changed the record: %+v", row)
	}
}

func TestConditionalUpdateCancellation(t *testing.T) {
	t.Run("before dispatch", func(t *testing.T) {
		m, id, _ := conditionalFixture(t)
		commands := watchConditional(t, m, nil)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		patch := &conditionalObject{ID: id}
		err := m.Update(manipulate.NewContext(ctx, manipulate.ContextOptionConditionalUpdate(conditionalSpec(7))), patch)
		if !errors.Is(err, context.Canceled) || len(commands) != 0 {
			t.Fatalf("cancelled request reached wire: %v, commands=%d", err, len(commands))
		}
	})
	t.Run("after dispatch", func(t *testing.T) {
		m, id, _ := conditionalFixture(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		commands := watchConditional(t, m, func() { time.AfterFunc(20*time.Millisecond, cancel) })
		conditionalFailpoint(t, m, bson.M{"failCommands": bson.A{"update"}, "blockConnection": true, "blockTimeMS": 200})
		patch := &conditionalObject{ID: id}
		input := *patch
		err := m.Update(manipulate.NewContext(ctx,
			manipulate.ContextOptionNamespace("/owned"),
			manipulate.ContextOptionConditionalUpdate(conditionalSpec(7)),
		), patch)
		if !errors.Is(err, context.Canceled) || *patch != input {
			t.Fatalf("post-dispatch cancellation: %v", err)
		}
		requireConditionalCommand(t, commands)
	})
}

type conditionalEncrypter struct {
	onEncrypt func()
}

func (e conditionalEncrypter) EncryptString(value string) (string, error) {
	if e.onEncrypt != nil {
		e.onEncrypt()
	}
	return "cipher:" + value, nil
}
func (conditionalEncrypter) DecryptString(string) (string, error) {
	return "", errors.New("unexpected decryption in a write-only update")
}

func TestConditionalUpdateEncryptsWithoutChangingPatch(t *testing.T) {
	m, id, collection := conditionalFixture(t, OptionAttributeEncrypter(conditionalEncrypter{}))
	patch := &conditionalObject{ID: id, Secret: conditionalPointer("private")}
	input := *patch
	if err := m.Update(conditionalContext(t, conditionalSpec(7)), patch); err != nil || *patch != input || *patch.Secret != "private" {
		t.Fatalf("write did not preserve the cleartext input: %+v, %v", patch, err)
	}
	if row := readConditional(t, collection, id); row["rev"] != int64(8) || row["secret"] != "cipher:private" {
		t.Fatalf("stored revision or ciphertext is wrong: %+v", row)
	}
}

type ConditionalCodecTestModel = conditionalObject

type conditionalPrivateCodecPatch struct {
	ConditionalCodecTestModel
	name string
}

func (p *conditionalPrivateCodecPatch) GetBSON() (any, error) {
	if p.name == "" {
		return bson.M{}, nil
	}
	return bson.M{"name": p.name}, nil
}

func TestConditionalUpdatePreservesPrivateCodecState(t *testing.T) {
	m, id, collection := conditionalFixture(t)
	commands := watchConditional(t, m, nil)
	patch := &conditionalPrivateCodecPatch{ConditionalCodecTestModel: conditionalObject{ID: id}, name: "saved"}
	if err := m.Update(conditionalContext(t, conditionalSpec(7)), patch); err != nil {
		t.Fatal(err)
	}
	requireConditionalCommand(t, commands)
	if row := readConditional(t, collection, id); row["name"] != "saved" || row["rev"] != int64(8) {
		t.Fatalf("successful update lost codec state: name=%v revision=%v", row["name"], row["rev"])
	}
	if patch.name != "saved" || patch.Identifier() != id {
		t.Fatalf("conditional update changed the patch: %+v", patch)
	}
}

func TestConditionalUpdateRejectsLossyCopyBeforeWrite(t *testing.T) {
	m, id, collection := conditionalFixture(t, OptionAttributeEncrypter(conditionalEncrypter{}))
	commands := watchConditional(t, m, nil)
	patch := &conditionalPrivateCodecPatch{ConditionalCodecTestModel: conditionalObject{ID: id}, name: "saved"}
	err := m.Update(conditionalContext(t, conditionalSpec(7)), patch)
	if !manipulate.IsCannotMarshalError(err) || len(commands) != 0 {
		t.Fatalf("lossy copy returned %v and issued %d database commands", err, len(commands))
	}
	if row := readConditional(t, collection, id); row["name"] != "before" || row["rev"] != int64(7) {
		t.Fatalf("rejected update changed the record: name=%v revision=%v", row["name"], row["rev"])
	}
}

func TestConditionalUpdateKeepsValidatedWriteConcern(t *testing.T) {
	m, id, collection := conditionalFixture(t)
	commands := watchConditional(t, m, nil)
	m.attributeEncrypter = conditionalEncrypter{onEncrypt: func() {
		if err := SetConsistencyMode(m, manipulate.ReadConsistencyDefault, manipulate.WriteConsistencyNone); err != nil {
			t.Fatal(err)
		}
	}}
	patch := &conditionalObject{ID: id, Secret: conditionalPointer("private")}
	if err := m.Update(conditionalContext(t, conditionalSpec(7)), patch); err != nil {
		t.Fatalf("in-flight update lost its acknowledged write concern: %v", err)
	}
	wc, ok := requireConditionalCommand(t, commands).Lookup("writeConcern").DocumentOK()
	if w, integer := wc.Lookup("w").AsInt64OK(); !ok || !integer || w != 1 {
		t.Fatalf("expected the in-flight update to keep w=1, got %v", wc)
	}
	if row := readConditional(t, collection, id); row["rev"] != int64(8) || row["secret"] != "cipher:private" {
		t.Fatalf("in-flight update did not commit: %+v", row)
	}
	if err := m.Update(conditionalContext(t, conditionalSpec(8)), &conditionalObject{ID: id}); !manipulate.IsCannotBuildQueryError(err) || len(commands) != 0 {
		t.Fatalf("next update did not reject the new unacknowledged default: %v, commands=%d", err, len(commands))
	}
}

type conditionalSharder struct{}

func (conditionalSharder) Shard(manipulate.TransactionalManipulator, manipulate.Context, elemental.Identifiable) error {
	return nil
}
func (conditionalSharder) OnShardedWrite(manipulate.TransactionalManipulator, manipulate.Context, elemental.Operation, elemental.Identifiable) error {
	return nil
}
func (conditionalSharder) FilterOne(_ manipulate.TransactionalManipulator, _ manipulate.Context, object elemental.Identifiable) (bson.D, error) {
	shard := bson.D{{Key: "zone", Value: 1}}
	if patch, ok := object.(*conditionalObject); ok && patch.ZHash != nil {
		shard = append(shard, bson.E{Key: "zhash", Value: *patch.ZHash})
	}
	return shard, nil
}
func (conditionalSharder) FilterMany(manipulate.TransactionalManipulator, manipulate.Context, elemental.Identity) (bson.D, error) {
	return nil, nil
}

func TestConditionalUpdateScopeAndQuery(t *testing.T) {
	for _, tc := range []struct {
		name    string
		opts    []Option
		context []manipulate.ContextOption
	}{
		{"namespace", nil, []manipulate.ContextOption{manipulate.ContextOptionNamespace("/other")}},
		{"query", nil, []manipulate.ContextOption{manipulate.ContextOptionFilter(elemental.NewFilter().WithKey("optional").Equals("different").Done())}},
		{"forced filter", []Option{OptionForceReadFilter(bson.D{{Key: "name", Value: "other"}})}, nil},
		{"shard", []Option{OptionSharder(conditionalSharder{})}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, id, collection := conditionalFixture(t, tc.opts...)
			oid, _ := bson.ObjectIDFromHex(id)
			before, err := collection.FindOne(t.Context(), bson.M{"_id": oid}).Raw()
			if err != nil {
				t.Fatal(err)
			}
			patch := &conditionalObject{ID: id, Namespace: conditionalPointer("/owned"), ZHash: conditionalPointer(int64(42)), Name: conditionalPointer("must not save")}
			input := *patch
			ctx := conditionalContext(t, conditionalSpec(7), tc.context...)
			err = m.Update(ctx, patch)
			if !manipulate.IsPreconditionFailedError(err) {
				t.Fatalf("scope miss returned %v", err)
			}
			after, err := collection.FindOne(t.Context(), bson.M{"_id": oid}).Raw()
			if err != nil || !bytes.Equal(before, after) || *patch != input {
				t.Fatalf("rejected scope changed data: %v", err)
			}
		})
	}
}

func TestConditionalUpdateRequiresExactRouting(t *testing.T) {
	m, id, collection := conditionalFixture(t, OptionSharder(conditionalSharder{}))
	oid, _ := bson.ObjectIDFromHex(id)
	if _, err := collection.UpdateOne(t.Context(), bson.M{"_id": oid}, bson.M{"$set": bson.M{"zone": int64(1), "zhash": int64(42)}}); err != nil {
		t.Fatal(err)
	}
	commands := watchConditional(t, m, nil)
	ctx := conditionalContext(t, conditionalSpec(7),
		manipulate.ContextOptionNamespace("/"),
		manipulate.ContextOptionRecursive(true),
		manipulate.ContextOptionFilter(elemental.NewFilter().WithKey("optional").Equals("keep-me").Done()),
	)
	for name, patch := range map[string]*conditionalObject{
		"missing namespace": {ID: id, ZHash: conditionalPointer(int64(42))},
		"missing hash":      {ID: id, Namespace: conditionalPointer("/owned")},
		"mismatched route":  {ID: id, Namespace: conditionalPointer("/owned"), Zone: conditionalPointer(int64(2)), ZHash: conditionalPointer(int64(42))},
	} {
		if err := m.Update(ctx, patch); !manipulate.IsCannotBuildQueryError(err) || len(commands) != 0 {
			t.Fatalf("%s should fail before dispatch: %v", name, err)
		}
	}

	patch := &conditionalObject{ID: id, Namespace: conditionalPointer("/owned"), ZHash: conditionalPointer(int64(42)), Name: conditionalPointer("next")}
	routeAsRevision := manipulate.ConditionalUpdate{RevisionField: "zone", ExpectedRevision: 1}
	if err := m.Update(ctx.Derive(manipulate.ContextOptionConditionalUpdate(routeAsRevision)), patch); !manipulate.IsCannotBuildQueryError(err) || len(commands) != 0 {
		t.Fatalf("accepted a shard field as the revision: %v", err)
	}
	if err := m.Update(ctx, patch); err != nil {
		t.Fatalf("valid scope, shard and translated query rejected: %v", err)
	}
	requireConditionalCommand(t, commands)
	row := readConditional(t, collection, id)
	if row["rev"] != int64(8) || row["zone"] != int64(1) || row["zhash"] != int64(42) || row["namespace"] != "/owned" {
		t.Fatalf("conditional patch changed routing fields: %+v", row)
	}
}

func TestConditionalUpdateMalformedStoredScalars(t *testing.T) {
	for _, tc := range []struct {
		name, key string
		value     any
	}{
		{"revision array", "rev", bson.A{int64(7)}},
		{"revision float", "rev", float64(7)},
		{"revision string", "rev", "7"},
		{"revision null", "rev", nil},
		{"namespace array", "namespace", bson.A{"/owned"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, id, collection := conditionalFixture(t)
			oid, _ := bson.ObjectIDFromHex(id)
			if _, err := collection.UpdateOne(t.Context(), bson.M{"_id": oid}, bson.M{"$set": bson.M{tc.key: tc.value}}); err != nil {
				t.Fatal(err)
			}
			before, err := collection.FindOne(t.Context(), bson.M{"_id": oid}).Raw()
			if err != nil {
				t.Fatal(err)
			}
			patch := &conditionalObject{ID: id, Name: conditionalPointer("must not save")}
			if err := m.Update(conditionalContext(t, conditionalSpec(7)), patch); !manipulate.IsPreconditionFailedError(err) {
				t.Fatalf("malformed scalar accepted: %v", err)
			}
			after, err := collection.FindOne(t.Context(), bson.M{"_id": oid}).Raw()
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("malformed storage changed: %v", err)
			}
		})
	}
}

func TestConditionalUpdateMatchesTargetCaseSensitively(t *testing.T) {
	const namespace = "/Owned.Team+"
	for _, tc := range []struct {
		name, storedNamespace, contextNamespace string
		patchNamespace                          bool
		stringID, recursive, propagated, hidden bool
	}{
		{name: "omitted patch namespace", storedNamespace: namespace, contextNamespace: namespace},
		{name: "explicit patch namespace", storedNamespace: namespace, contextNamespace: namespace, patchNamespace: true},
		{name: "recursive scope root", storedNamespace: namespace, contextNamespace: namespace, patchNamespace: true, recursive: true},
		{name: "recursive descendant", storedNamespace: namespace + "/Child", contextNamespace: namespace, patchNamespace: true, recursive: true},
		{name: "propagated ancestor", storedNamespace: namespace, contextNamespace: namespace + "/Child", patchNamespace: true, propagated: true},
		{name: "hidden propagated ancestor", storedNamespace: namespace, contextNamespace: namespace + "/Child", patchNamespace: true, propagated: true, hidden: true},
		{name: "string identifier", storedNamespace: namespace, contextNamespace: namespace, stringID: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			uri, db := requireMemongo(t)
			storage, err := New(uri, db, OptionForceReadFilter(bson.D{{Key: "opt", Value: "TINTED"}}))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = Disconnect(storage, t.Context()) })
			database := storage.(*mongoManipulator).client.Database(db)
			if err := database.CreateCollection(t.Context(), conditionalIdentity.Name,
				options.CreateCollection().SetCollation(&options.Collation{Locale: "en", Strength: 2}),
			); err != nil {
				t.Fatal(err)
			}
			id := bson.NewObjectID()
			recordID, patchID := any(id), id.Hex()
			wrongNamespace, wrongID := strings.ToLower(tc.contextNamespace), patchID
			if tc.stringID {
				patchID = "Record.Team+"
				recordID, wrongNamespace, wrongID = patchID, tc.contextNamespace, strings.ToLower(patchID)
			}
			collection := database.Collection(conditionalIdentity.Name)
			if _, err := collection.InsertOne(t.Context(), bson.M{
				"_id": recordID, "namespace": tc.storedNamespace, "name": "Blue", "opt": "tinted", "rev": int64(7),
				"propagate": tc.propagated, "hidden": tc.hidden,
			}); err != nil {
				t.Fatal(err)
			}
			ctx := conditionalContext(t, conditionalSpec(7),
				manipulate.ContextOptionNamespace(tc.contextNamespace),
				manipulate.ContextOptionRecursive(tc.recursive),
				manipulate.ContextOptionPropagated(tc.propagated),
				manipulate.ContextOptionFilter(elemental.NewFilter().WithKey("name").Equals("BLUE").Done()),
			)
			patch := &conditionalObject{ID: wrongID, Name: conditionalPointer("saved")}
			if tc.patchNamespace {
				patch.Namespace = conditionalPointer(tc.storedNamespace)
			}
			read := func() bson.M {
				t.Helper()
				var row bson.M
				if err := collection.FindOne(t.Context(), bson.M{"_id": recordID}).Decode(&row); err != nil {
					t.Fatal(err)
				}
				return row
			}
			visibleScope := ctx
			if tc.hidden {
				ctx = ctx.Derive(manipulate.ContextOptionPropagatedHidden(true))
			}
			err = storage.Update(ctx.Derive(manipulate.ContextOptionNamespace(wrongNamespace)), patch)
			if row := read(); !manipulate.IsPreconditionFailedError(err) || row["rev"] != int64(7) || row["name"] != "Blue" {
				t.Fatalf("case-only mismatch returned %v, stored revision=%v name=%v", err, row["rev"], row["name"])
			}
			patch.ID = patchID
			if tc.hidden {
				if err := storage.Update(visibleScope, patch); !manipulate.IsPreconditionFailedError(err) {
					t.Fatalf("hidden propagated target matched without opt-in: %v", err)
				}
			}
			if err := storage.Update(ctx, patch); err != nil {
				t.Fatalf("exact target with matching collated filters did not update: %v", err)
			}
			if row := read(); row["rev"] != int64(8) || row["name"] != "saved" || row["namespace"] != tc.storedNamespace {
				t.Fatalf("scoped update did not preserve the namespace and save the patch: %+v", row)
			}
		})
	}
}

func TestConditionalUpdateDeletedTarget(t *testing.T) {
	m, id, collection := conditionalFixture(t)
	if _, err := collection.DeleteMany(t.Context(), bson.D{}); err != nil {
		t.Fatal(err)
	}
	patch := &conditionalObject{ID: id}
	if err := m.Update(conditionalContext(t, conditionalSpec(7)), patch); !manipulate.IsPreconditionFailedError(err) {
		t.Fatalf("deleted target returned %v", err)
	}
	if count, err := collection.CountDocuments(t.Context(), bson.D{}); err != nil || count != 0 {
		t.Fatalf("conditional miss recreated target: %d, %v", count, err)
	}
}

func TestConditionalUpdateInvalidRequests(t *testing.T) {
	m, id, _ := conditionalFixture(t)
	commands := watchConditional(t, m, nil)
	for _, tc := range []struct {
		name   string
		change func(*conditionalObject, *manipulate.ConditionalUpdate)
	}{
		{"empty option", func(_ *conditionalObject, c *manipulate.ConditionalUpdate) { *c = manipulate.ConditionalUpdate{} }},
		{"unknown attribute", func(_ *conditionalObject, c *manipulate.ConditionalUpdate) { c.RevisionField = "missing" }},
		{"BSON key instead of attribute", func(_ *conditionalObject, c *manipulate.ConditionalUpdate) { c.RevisionField = "rev" }},
		{"string instead of integer", func(_ *conditionalObject, c *manipulate.ConditionalUpdate) { c.RevisionField = "name" }},
		{"negative revision", func(_ *conditionalObject, c *manipulate.ConditionalUpdate) { c.ExpectedRevision = -1 }},
		{"exhaustion", func(_ *conditionalObject, c *manipulate.ConditionalUpdate) { c.ExpectedRevision = math.MaxInt64 }},
		{"missing nonzero revision", func(_ *conditionalObject, c *manipulate.ConditionalUpdate) { c.AllowMissingAtZero = true }},
		{"patch proposes revision", func(p *conditionalObject, _ *manipulate.ConditionalUpdate) { p.Revision = conditionalPointer(int64(8)) }},
		{"maximum bound", func(p *conditionalObject, _ *manipulate.ConditionalUpdate) { p.MaxRevision = 7 }},
		{"minimum bound", func(p *conditionalObject, _ *manipulate.ConditionalUpdate) { p.MinRevision = 9 }},
		{"large maximum bound", func(p *conditionalObject, c *manipulate.ConditionalUpdate) {
			const bound = 1 << 53
			p.MaxRevision, c.ExpectedRevision = bound, bound
		}},
		{"large minimum bound", func(p *conditionalObject, c *manipulate.ConditionalUpdate) {
			const bound = 1<<53 + 4
			p.MinRevision, c.ExpectedRevision = bound, bound-2
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			patch := &conditionalObject{ID: id, Name: conditionalPointer("next")}
			spec := conditionalSpec(7)
			tc.change(patch, &spec)
			input := *patch
			if err := m.Update(conditionalContext(t, spec), patch); !manipulate.IsCannotBuildQueryError(err) || *patch != input {
				t.Fatalf("invalid CAS changed input or returned %v", err)
			}
		})
	}
	generatedWithoutRevision := testmodel.NewSparseTask()
	generatedWithoutRevision.SetIdentifier(id)
	for _, patch := range []elemental.Identifiable{nil, (*conditionalObject)(nil), &conditionalObject{}, newMongoIntegrationObject("full"), generatedWithoutRevision} {
		if err := m.Update(conditionalContext(t, conditionalSpec(7)), patch); !manipulate.IsCannotBuildQueryError(err) {
			t.Fatalf("invalid model accepted: %v", err)
		}
	}
	patch := &conditionalObject{ID: id}
	spec := conditionalSpec(7)
	ctx := conditionalContext(t, spec, manipulate.ContextOptionWriteConsistency(manipulate.WriteConsistencyNone))
	if err := m.Update(ctx, patch); !manipulate.IsCannotBuildQueryError(err) {
		t.Fatalf("unacknowledged write: %v", err)
	}
	ctx = conditionalContext(t, spec, manipulate.ContextOptionTransactionID("unsupported"))
	if err := m.Update(ctx, patch); !manipulate.IsNotImplementedError(err) {
		t.Fatalf("transaction: %v", err)
	}
	session, err := m.client.StartSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.EndSession(t.Context())
	ctx = manipulate.NewContext(mongo.NewSessionContext(t.Context(), session), manipulate.ContextOptionConditionalUpdate(spec))
	if err := m.Update(ctx, patch); !manipulate.IsNotImplementedError(err) {
		t.Fatalf("explicit Mongo session: %v", err)
	}
	if len(commands) != 0 {
		t.Fatalf("invalid request issued %d database commands", len(commands))
	}
}

func TestConditionalUpdateKeepsPatchSparse(t *testing.T) {
	m, id, collection := conditionalFixture(t)
	oid, _ := bson.ObjectIDFromHex(id)
	if _, err := collection.UpdateOne(t.Context(), bson.M{"_id": oid}, bson.M{"$set": bson.M{"unknown": "keep"}}); err != nil {
		t.Fatal(err)
	}
	commands := watchConditional(t, m, nil)
	patch := &conditionalObject{
		ID: id, Name: conditionalPointer(""),
		FirstSeen: conditionalPointer(time.Unix(1, 0)), ModelVersion: 17,
	}
	input := *patch
	ctx := conditionalContext(t, conditionalSpec(7), manipulate.ContextOptionFields([]string{"optional"}))
	if err := m.Update(ctx, patch); err != nil {
		t.Fatal(err)
	}
	requireConditionalCommand(t, commands)
	if *patch != input || *patch.Name != "" || !patch.FirstSeen.Equal(time.Unix(1, 0)) {
		t.Fatalf("conditional update changed the input patch: %+v", patch)
	}
	row := readConditional(t, collection, id)
	if row["unknown"] != "keep" || row["opt"] != "keep-me" || row["name"] != "" || row["rev"] != int64(8) ||
		row["first"] != bson.NewDateTimeFromTime(time.Unix(1000, 0)) ||
		row["last"] != bson.NewDateTimeFromTime(time.Unix(2000, 0)) {
		t.Fatalf("sparse CAS changed omitted or protected fields or lost an explicit zero: %+v", row)
	}
}

func TestConditionalUpdateCompetingSaves(t *testing.T) {
	m, id, collection := conditionalFixture(t)
	results := make(chan error, 2)
	var start sync.WaitGroup
	start.Add(1)
	for _, name := range []string{"first", "second"} {
		go func() {
			patch := &conditionalObject{ID: id, Name: conditionalPointer(name)}
			start.Wait()
			results <- m.Update(conditionalContext(t, conditionalSpec(7)), patch)
		}()
	}
	start.Done()
	winners, misses := 0, 0
	for range 2 {
		switch err := <-results; {
		case err == nil:
			winners++
		case manipulate.IsPreconditionFailedError(err):
			misses++
		default:
			t.Fatalf("unexpected competing-write result: %v", err)
		}
	}
	if winners != 1 || misses != 1 || readConditional(t, collection, id)["rev"] != int64(8) {
		t.Fatalf("competing writes had %d winners and %d misses", winners, misses)
	}
	count, err := collection.CountDocuments(t.Context(), bson.D{})
	if err != nil || count != 1 {
		t.Fatalf("conditional miss inserted a row: %d, %v", count, err)
	}
}

func TestConditionalUpdateIntegerRepresentations(t *testing.T) {
	m, id, collection := conditionalFixture(t)
	oid, _ := bson.ObjectIDFromHex(id)
	for _, tc := range []struct {
		stored   any
		expected int64
	}{
		{int32(7), 7},
		{int64(7), 7},
		{int64(0), 0},
		{int64(math.MaxInt64 - 1), math.MaxInt64 - 1},
	} {
		if _, err := collection.UpdateOne(t.Context(), bson.M{"_id": oid}, bson.M{"$set": bson.M{"rev": tc.stored}}); err != nil {
			t.Fatal(err)
		}
		patch := &conditionalObject{ID: id}
		if err := m.Update(conditionalContext(t, conditionalSpec(tc.expected)), patch); err != nil {
			t.Fatalf("revision-only CAS for %T(%v): %v", tc.stored, tc.stored, err)
		}
		if got := readConditional(t, collection, id)["rev"]; got != tc.expected+1 {
			t.Fatalf("stored revision after CAS for %T(%v): %v", tc.stored, tc.stored, got)
		}
	}
}

func TestConditionalUpdateEmptyNamespaceFence(t *testing.T) {
	m, id, collection := conditionalFixture(t)
	oid, _ := bson.ObjectIDFromHex(id)
	if _, err := collection.UpdateOne(t.Context(), bson.M{"_id": oid}, bson.M{"$unset": bson.M{"namespace": ""}}); err != nil {
		t.Fatal(err)
	}
	patch := &conditionalObject{ID: id, Name: conditionalPointer("first-save")}
	if err := m.Update(manipulate.NewContext(t.Context(), manipulate.ContextOptionConditionalUpdate(conditionalSpec(7))), patch); err != nil {
		t.Fatal(err)
	}
	next := &conditionalObject{ID: id, Name: conditionalPointer("must-not-save")}
	ctx := manipulate.NewContext(t.Context(), manipulate.ContextOptionConditionalUpdate(conditionalSpec(8)))
	for _, namespace := range []any{"/moved", nil, bson.A{""}} {
		if _, err := collection.UpdateOne(t.Context(), bson.M{"_id": oid}, bson.M{"$set": bson.M{"namespace": namespace}}); err != nil {
			t.Fatal(err)
		}
		if err := m.Update(ctx, next); !manipulate.IsPreconditionFailedError(err) {
			t.Fatalf("empty namespace crossed changed scope %v: %v", namespace, err)
		}
		if row := readConditional(t, collection, id); row["name"] != "first-save" || row["rev"] != int64(8) {
			t.Fatalf("rejected scope changed record: %+v", row)
		}
	}
}

func TestConditionalUpdateNewOperationDoesNotAdvanceRevision(t *testing.T) {
	m, id, coll := conditionalFixture(t)
	marked := conditionalContext(t, conditionalSpec(7))
	fresh := manipulate.NewContext(marked.Context(), manipulate.ContextOptionNamespace("/owned"))
	if err := m.Update(fresh, &conditionalObject{ID: id, Name: conditionalPointer("ordinary")}); err != nil {
		t.Fatal(err)
	}
	if row := readConditional(t, coll, id); row["rev"] != int64(7) || row["name"] != "ordinary" {
		t.Fatalf("unmarked update unexpectedly performed a CAS: %+v", row)
	}
}

func TestConditionalUpdateMissingRevisionRequiresExplicitZero(t *testing.T) {
	m, id, collection := conditionalFixture(t)
	oid, _ := bson.ObjectIDFromHex(id)
	if _, err := collection.UpdateOne(t.Context(), bson.M{"_id": oid}, bson.M{"$unset": bson.M{"rev": ""}}); err != nil {
		t.Fatal(err)
	}
	patch := &conditionalObject{ID: id}
	check := conditionalSpec(0)
	if err := m.Update(conditionalContext(t, check), patch); !manipulate.IsPreconditionFailedError(err) {
		t.Fatalf("absent revision matched strict zero: %v", err)
	}
	check.AllowMissingAtZero = true
	if err := m.Update(conditionalContext(t, check), patch); err != nil {
		t.Fatalf("missing legacy revision was rejected: %v", err)
	}
	if got := readConditional(t, collection, id)["rev"]; got != int64(1) {
		t.Fatalf("missing legacy revision did not advance to one: %v", got)
	}
	for _, invalid := range []any{int64(1), nil, float64(0), bson.A{int64(0)}} {
		if _, err := collection.UpdateOne(t.Context(), bson.M{"_id": oid}, bson.M{"$set": bson.M{"rev": invalid}}); err != nil {
			t.Fatal(err)
		}
		if err := m.Update(conditionalContext(t, check), &conditionalObject{ID: id}); !manipulate.IsPreconditionFailedError(err) {
			t.Fatalf("legacy zero matched stored %T: %v", invalid, err)
		}
	}
}
