package manipmongo

import (
	"context"
	"crypto/tls"
	"reflect"
	"testing"
	"time"

	"go.acuvity.ai/elemental"
	"go.acuvity.ai/manipulate"
	mongobson "go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mongooptions "go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestOptionBuilders(t *testing.T) {
	c := newConfig()
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	sharder := &testOptionsSharder{}
	identity := elemental.MakeIdentity("resource", "resources")

	OptionCredentials("user", "pass", "auth")(c)
	OptionTLS(tlsConfig)(c)
	OptionConnectionPoolLimit(17)(c)
	OptionConnectionTimeout(2 * time.Second)(c)
	OptionClientTimeout(3 * time.Second)(c)
	OptionSocketTimeout(4 * time.Second)(c)
	OptionDefaultReadConsistencyMode(manipulate.ReadConsistencyNearest)(c)
	OptionDefaultWriteConsistencyMode(manipulate.WriteConsistencyStrong)(c)
	OptionSharder(sharder)(c)
	OptionDefaultRetryFunc(func(manipulate.RetryInfo) error { return nil })(c)
	OptionForceReadFilter(mongobson.D{{Key: "tenant", Value: "acuvity"}})(c)
	OptionAttributeEncrypter(&helperTestAttributeEncrypter{})(c)
	OptionExplain(map[elemental.Identity]map[elemental.Operation]struct{}{identity: {elemental.OperationCreate: {}}})(c)

	if c.username != "user" || c.password != "pass" || c.authsource != "auth" {
		t.Fatalf("credentials not applied: %+v", c)
	}
	if c.tlsConfig != tlsConfig || c.poolLimit != 17 || c.connectTimeout != 2*time.Second || c.clientTimeout != 3*time.Second || c.operationTimeout != 4*time.Second {
		t.Fatalf("connection options not applied: %+v", c)
	}
	if c.readConsistency != manipulate.ReadConsistencyNearest || c.writeConsistency != manipulate.WriteConsistencyStrong {
		t.Fatalf("consistency options not applied: %+v", c)
	}
	if c.sharder != sharder {
		t.Fatalf("sharder not applied")
	}
	if len(c.forcedReadFilter) != 1 || c.attributeEncrypter == nil || c.explain == nil {
		t.Fatalf("expected optional values to be configured: %+v", c)
	}
}

func TestOptionTranslateKeysFromModelManagerPanicsOnNil(t *testing.T) {
	assertPanics(t, func() { OptionTranslateKeysFromModelManager(nil) })
}

func TestContextOptionUpsert(t *testing.T) {
	ops := mongobson.M{"$setOnInsert": mongobson.M{"name": "value"}}
	mctx := manipulate.NewContext(context.Background())
	ContextOptionUpsert(ops)(mctx)
	if got := mctx.(opaquer).Opaque()[opaqueKeyUpsert]; !reflect.DeepEqual(got, ops) {
		t.Fatalf("unexpected upsert operations: %#v", got)
	}
}

func TestContextOptionUpsertPanicsOnInvalidInput(t *testing.T) {
	cases := []mongobson.M{
		{"$set": true},
		{"$setOnInsert": "bad"},
		{"$setOnInsert": mongobson.M{"_id": 1}},
	}

	for _, tc := range cases {
		assertPanics(t, func() { ContextOptionUpsert(tc) })
	}
}

func TestNewAllowsZeroConnectionTimeout(t *testing.T) {
	originalConnect := mongoConnectFn
	originalPing := mongoPingFn
	originalDisconnect := mongoDisconnectFn
	defer func() {
		mongoConnectFn = originalConnect
		mongoPingFn = originalPing
		mongoDisconnectFn = originalDisconnect
	}()

	connectCalls := 0
	pingCalls := 0
	mongoConnectFn = func(opts ...*mongooptions.ClientOptions) (*mongo.Client, error) {
		connectCalls++
		return &mongo.Client{}, nil
	}
	mongoPingFn = func(client *mongo.Client, ctx context.Context) error {
		pingCalls++
		if err := ctx.Err(); err != nil {
			t.Fatalf("expected ping context to be active, got %v", err)
		}
		if _, ok := ctx.Deadline(); ok {
			t.Fatalf("expected zero connection timeout to avoid setting a deadline")
		}
		return nil
	}
	mongoDisconnectFn = func(*mongo.Client, context.Context) error { return nil }

	manipulator, err := New("mongodb://127.0.0.1:27017", "test", OptionConnectionTimeout(0))
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}
	if manipulator == nil {
		t.Fatalf("expected non-nil manipulator")
	}
	if connectCalls != 1 || pingCalls != 1 {
		t.Fatalf("expected one connect and one ping call, got connect=%d ping=%d", connectCalls, pingCalls)
	}
}

type testOptionsSharder struct{}

func (*testOptionsSharder) Shard(manipulate.TransactionalManipulator, manipulate.Context, elemental.Identifiable) error {
	return nil
}
func (*testOptionsSharder) OnShardedWrite(manipulate.TransactionalManipulator, manipulate.Context, elemental.Operation, elemental.Identifiable) error {
	return nil
}
func (*testOptionsSharder) FilterOne(manipulate.TransactionalManipulator, manipulate.Context, elemental.Identifiable) (mongobson.D, error) {
	return nil, nil
}
func (*testOptionsSharder) FilterMany(manipulate.TransactionalManipulator, manipulate.Context, elemental.Identity) (mongobson.D, error) {
	return nil, nil
}
