package manipmongo

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.acuvity.ai/manipulate"
	mongobson "go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type timeoutNetError struct{}

func (timeoutNetError) Error() string   { return "i/o timeout" }
func (timeoutNetError) Timeout() bool   { return true }
func (timeoutNetError) Temporary() bool { return true }

func TestConvertUpsertOperationsToMongo(t *testing.T) {
	tests := []struct {
		name      string
		in        any
		expectErr bool
	}{
		{
			name: "mongo bson",
			in: mongobson.M{
				"$setOnInsert": mongobson.M{"name": "demo"},
			},
		},
		{
			name:      "invalid type",
			in:        "invalid",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := convertUpsertOperationsToMongo(tt.in)
			if tt.expectErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if out == nil {
				t.Fatalf("expected non-nil converted map")
			}
			if _, ok := out["$setOnInsert"]; !ok {
				t.Fatalf("expected $setOnInsert key in converted map, got: %#v", out)
			}
		})
	}
}

func TestHandleQueryErrorMongo(t *testing.T) {
	tests := []struct {
		name      string
		in        error
		assertion func(error) bool
	}{
		{
			name: "nil",
			in:   nil,
			assertion: func(err error) bool {
				return err == nil
			},
		},
		{
			name: "context deadline",
			in:   context.DeadlineExceeded,
			assertion: func(err error) bool {
				return manipulate.IsCannotCommunicateError(err)
			},
		},
		{
			name: "not found",
			in:   mongo.ErrNoDocuments,
			assertion: func(err error) bool {
				return manipulate.IsObjectNotFoundError(err)
			},
		},
		{
			name: "network timeout",
			in:   timeoutNetError{},
			assertion: func(err error) bool {
				return manipulate.IsCannotCommunicateError(err)
			},
		},
		{
			name: "invalid query bad regex",
			in: mongo.CommandError{
				Code:    2,
				Message: errInvalidQueryBadRegex,
			},
			assertion: func(err error) bool {
				var invErr manipulate.ErrInvalidQuery
				return errors.As(err, &invErr)
			},
		},
		{
			name: "invalid query invalid regex",
			in: mongo.CommandError{
				Code:    51091,
				Message: errInvalidQueryInvalidRegex,
			},
			assertion: func(err error) bool {
				var invErr manipulate.ErrInvalidQuery
				return errors.As(err, &invErr)
			},
		},
		{
			name: "generic error",
			in:   errors.New("boom"),
			assertion: func(err error) bool {
				return manipulate.IsCannotExecuteQueryError(err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := HandleQueryErrorMongo(tt.in)
			if !tt.assertion(err) {
				t.Fatalf("unexpected error classification for %q: %v", tt.name, err)
			}
		})
	}
}

func TestNewMongoDisconnectsClientWhenPingFails(t *testing.T) {
	previousConnect := mongoConnectFn
	previousPing := mongoPingFn
	previousDisconnect := mongoDisconnectFn
	defer func() {
		mongoConnectFn = previousConnect
		mongoPingFn = previousPing
		mongoDisconnectFn = previousDisconnect
	}()

	pingErr := errors.New("ping failed")
	disconnectCalls := 0

	mongoConnectFn = func(...*options.ClientOptions) (*mongo.Client, error) {
		return &mongo.Client{}, nil
	}
	mongoPingFn = func(*mongo.Client, context.Context) error {
		return pingErr
	}
	mongoDisconnectFn = func(*mongo.Client, context.Context) error {
		disconnectCalls++
		return nil
	}

	_, err := New("mongodb://127.0.0.1:27017", "db")
	if err == nil || !strings.Contains(err.Error(), "cannot ping mongo url") {
		t.Fatalf("expected ping failure from constructor, got: %v", err)
	}

	if disconnectCalls != 1 {
		t.Fatalf("expected disconnect to be called once on ping failure, got %d", disconnectCalls)
	}
}

func TestNewMongoRedactsCredentialsInConnectError(t *testing.T) {
	previousConnect := mongoConnectFn
	previousPing := mongoPingFn
	previousDisconnect := mongoDisconnectFn
	defer func() {
		mongoConnectFn = previousConnect
		mongoPingFn = previousPing
		mongoDisconnectFn = previousDisconnect
	}()

	mongoConnectFn = func(...*options.ClientOptions) (*mongo.Client, error) {
		return nil, errors.New("connect failed")
	}
	mongoPingFn = func(*mongo.Client, context.Context) error { return nil }
	mongoDisconnectFn = func(*mongo.Client, context.Context) error { return nil }

	_, err := New("mongodb://user:pass@127.0.0.1:27017/admin", "db")
	if err == nil {
		t.Fatalf("expected constructor connect error")
	}
	if !strings.Contains(err.Error(), "cannot connect to mongo url") {
		t.Fatalf("expected connect error prefix, got: %v", err)
	}
	if strings.Contains(err.Error(), "user:pass@") || strings.Contains(err.Error(), "pass@") {
		t.Fatalf("expected credential redaction in error, got: %v", err)
	}
}

func TestNewMongoRedactsCredentialsInPingError(t *testing.T) {
	previousConnect := mongoConnectFn
	previousPing := mongoPingFn
	previousDisconnect := mongoDisconnectFn
	defer func() {
		mongoConnectFn = previousConnect
		mongoPingFn = previousPing
		mongoDisconnectFn = previousDisconnect
	}()

	mongoConnectFn = func(...*options.ClientOptions) (*mongo.Client, error) {
		return &mongo.Client{}, nil
	}
	mongoPingFn = func(*mongo.Client, context.Context) error {
		return errors.New("ping failed")
	}
	mongoDisconnectFn = func(*mongo.Client, context.Context) error { return nil }

	_, err := New("mongodb://user:pass@127.0.0.1:27017/admin", "db")
	if err == nil {
		t.Fatalf("expected constructor ping error")
	}
	if !strings.Contains(err.Error(), "cannot ping mongo url") {
		t.Fatalf("expected ping error prefix, got: %v", err)
	}
	if strings.Contains(err.Error(), "user:pass@") || strings.Contains(err.Error(), "pass@") {
		t.Fatalf("expected credential redaction in error, got: %v", err)
	}
}

func TestNewMongoMapsZeroConnectionPoolLimitToDefault(t *testing.T) {
	previousConnect := mongoConnectFn
	previousPing := mongoPingFn
	previousDisconnect := mongoDisconnectFn
	defer func() {
		mongoConnectFn = previousConnect
		mongoPingFn = previousPing
		mongoDisconnectFn = previousDisconnect
	}()

	var capturedOpts *options.ClientOptions
	mongoConnectFn = func(opts ...*options.ClientOptions) (*mongo.Client, error) {
		if len(opts) != 1 {
			t.Fatalf("expected one client options argument, got %d", len(opts))
		}
		capturedOpts = opts[0]
		return &mongo.Client{}, nil
	}
	mongoPingFn = func(*mongo.Client, context.Context) error { return nil }
	mongoDisconnectFn = func(*mongo.Client, context.Context) error { return nil }

	out, err := New(
		"mongodb://127.0.0.1:27017",
		"db",
		OptionConnectionPoolLimit(0),
	)
	if err != nil {
		t.Fatalf("unexpected constructor error: %v", err)
	}
	if capturedOpts == nil || capturedOpts.MaxPoolSize == nil {
		t.Fatalf("expected constructor to set max pool size")
	}
	if *capturedOpts.MaxPoolSize != uint64(newConfig().poolLimit) {
		t.Fatalf("expected max pool size %d for zero pool limit, got %d", newConfig().poolLimit, *capturedOpts.MaxPoolSize)
	}
	if _, ok := out.(*mongoDriverManipulator); !ok {
		t.Fatalf("expected *mongoDriverManipulator, got %T", out)
	}
}

func TestNewMongoCachesForcedReadFilterFromMongoOption(t *testing.T) {
	previousConnect := mongoConnectFn
	previousPing := mongoPingFn
	previousDisconnect := mongoDisconnectFn
	defer func() {
		mongoConnectFn = previousConnect
		mongoPingFn = previousPing
		mongoDisconnectFn = previousDisconnect
	}()

	mongoConnectFn = func(...*options.ClientOptions) (*mongo.Client, error) {
		return &mongo.Client{}, nil
	}
	mongoPingFn = func(*mongo.Client, context.Context) error { return nil }
	mongoDisconnectFn = func(*mongo.Client, context.Context) error { return nil }

	mapi, err := New(
		"mongodb://127.0.0.1:27017",
		"db",
		optionForceReadFilterCanonical(mongobson.D{{Key: "status", Value: "todo"}}),
	)
	if err != nil {
		t.Fatalf("unexpected constructor error: %v", err)
	}

	m, ok := mapi.(*mongoDriverManipulator)
	if !ok {
		t.Fatalf("expected *mongoDriverManipulator, got %T", mapi)
	}

	if m.forcedReadFilter == nil || len(m.forcedReadFilter) != 1 {
		t.Fatalf("expected cached mongo forced filter, got %#v", m.forcedReadFilter)
	}
	if m.forcedReadFilter[0].Key != "status" || m.forcedReadFilter[0].Value != "todo" {
		t.Fatalf("unexpected cached mongo forced filter content: %#v", m.forcedReadFilter)
	}
}

func TestNewMongoRejectsInvalidMongoForcedReadFilterBeforeConnect(t *testing.T) {
	previousConnect := mongoConnectFn
	previousPing := mongoPingFn
	previousDisconnect := mongoDisconnectFn
	defer func() {
		mongoConnectFn = previousConnect
		mongoPingFn = previousPing
		mongoDisconnectFn = previousDisconnect
	}()

	connectCalls := 0
	mongoConnectFn = func(...*options.ClientOptions) (*mongo.Client, error) {
		connectCalls++
		return &mongo.Client{}, nil
	}
	mongoPingFn = func(*mongo.Client, context.Context) error { return nil }
	mongoDisconnectFn = func(*mongo.Client, context.Context) error { return nil }

	_, err := New(
		"mongodb://127.0.0.1:27017",
		"db",
		optionForceReadFilterCanonical(mongobson.D{{Key: "bad", Value: func() {}}}),
	)
	if err == nil {
		t.Fatalf("expected forced read filter validation error")
	}
	if connectCalls != 0 {
		t.Fatalf("expected constructor to fail before connect, got %d connect calls", connectCalls)
	}
}

func TestNewMongoCachesMongoForcedReadFilterFromEmptyOption(t *testing.T) {
	previousConnect := mongoConnectFn
	previousPing := mongoPingFn
	previousDisconnect := mongoDisconnectFn
	defer func() {
		mongoConnectFn = previousConnect
		mongoPingFn = previousPing
		mongoDisconnectFn = previousDisconnect
	}()

	mongoConnectFn = func(...*options.ClientOptions) (*mongo.Client, error) {
		return &mongo.Client{}, nil
	}
	mongoPingFn = func(*mongo.Client, context.Context) error { return nil }
	mongoDisconnectFn = func(*mongo.Client, context.Context) error { return nil }

	mapi, err := New(
		"mongodb://127.0.0.1:27017",
		"db",
		optionForceReadFilterCanonical(mongobson.D{}),
	)
	if err != nil {
		t.Fatalf("unexpected constructor error: %v", err)
	}

	m, ok := mapi.(*mongoDriverManipulator)
	if !ok {
		t.Fatalf("expected *mongoDriverManipulator, got %T", mapi)
	}
	if m.forcedReadFilter == nil {
		t.Fatalf("expected cached mongo forced read filter to be non-nil")
	}
	if len(m.forcedReadFilter) != 0 {
		t.Fatalf("expected empty forced read filter, got mongo=%#v", m.forcedReadFilter)
	}
}
