package manipmongo

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"go.acuvity.ai/elemental"
	"go.acuvity.ai/manipulate"
	mongobson "go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type testAttributeSpec struct {
	fields map[string]string
}

func (s *testAttributeSpec) AttributeSpecifications() map[string]elemental.AttributeSpecification {
	return nil
}
func (s *testAttributeSpec) ValueForAttribute(string) any { return nil }
func (s *testAttributeSpec) SpecificationForAttribute(name string) elemental.AttributeSpecification {
	if bsonField, ok := s.fields[name]; ok {
		return elemental.AttributeSpecification{BSONFieldName: bsonField}
	}
	return elemental.AttributeSpecification{}
}

type timeoutNetError struct{ message string }

func (e timeoutNetError) Error() string   { return e.message }
func (e timeoutNetError) Timeout() bool   { return true }
func (e timeoutNetError) Temporary() bool { return false }

type errorKind string

const (
	errorKindNil                errorKind = "nil"
	errorKindCannotCommunicate  errorKind = "cannot_communicate"
	errorKindCannotExecuteQuery errorKind = "cannot_execute_query"
	errorKindConstraint         errorKind = "constraint_violation"
	errorKindInvalidQuery       errorKind = "invalid_query"
	errorKindObjectNotFound     errorKind = "object_not_found"
)

func TestApplyOrdering(t *testing.T) {
	spec := &testAttributeSpec{fields: map[string]string{"Name": "displayname"}}
	got := applyOrdering([]string{"Name", "-ID"}, spec)
	want := []string{"displayname", "-id"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected ordering: got %v want %v", got, want)
	}

	got = applyOrdering([]string{"id", "-id"}, nil)
	want = []string{"_id", "-_id"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected fallback ordering: got %v want %v", got, want)
	}
}

func TestMakePipeline(t *testing.T) {
	afterID := mongobson.NewObjectID()
	pipe, err := makePipeline(
		nil,
		func(id mongobson.ObjectID) (mongobson.M, error) {
			if id != afterID {
				t.Fatalf("unexpected retriever id: got %s want %s", id.Hex(), afterID.Hex())
			}
			return mongobson.M{"name": "demo"}, nil
		},
		mongobson.D{{Key: "tenant", Value: "acuvity"}},
		mongobson.D{{Key: "namespace", Value: "/ns"}},
		mongobson.D{{Key: "active", Value: true}},
		mongobson.D{{Key: "status", Value: "ready"}},
		[]string{"name"},
		afterID.Hex(),
		5,
		[]string{"name"},
	)
	if err != nil {
		t.Fatalf("makePipeline returned unexpected error: %v", err)
	}
	if len(pipe) != 8 {
		t.Fatalf("unexpected pipeline length: got %d want 8", len(pipe))
	}
	if pipe[0][0].Key != "$match" {
		t.Fatalf("unexpected first stage: %#v", pipe[0])
	}
}

func TestMakePipelineInvalidAfter(t *testing.T) {
	_, err := makePipeline(nil, func(mongobson.ObjectID) (mongobson.M, error) { return nil, nil }, nil, nil, nil, nil, nil, "bad-after", 0, nil)
	if err == nil {
		t.Fatalf("expected invalid after error")
	}
	var invalid manipulate.ErrInvalidQuery
	if !errors.As(err, &invalid) && !strings.Contains(err.Error(), "parsable objectId") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHandleQueryErrorClassifications(t *testing.T) {
	mappers := []struct {
		name string
		fn   func(error) error
	}{
		{name: "HandleQueryError", fn: HandleQueryError},
		{name: "HandleQueryErrorMongo", fn: HandleQueryErrorMongo},
	}

	tests := []struct {
		name string
		err  error
		want errorKind
	}{
		{name: "nil", err: nil, want: errorKindNil},
		{name: "operation context deadline exceeded", err: &mongoOperationContextError{Err: context.DeadlineExceeded}, want: errorKindCannotExecuteQuery},
		{name: "context deadline exceeded", err: context.DeadlineExceeded, want: errorKindCannotCommunicate},
		{name: "context canceled", err: context.Canceled, want: errorKindCannotCommunicate},
		{name: "network timeout", err: timeoutNetError{message: "timeout"}, want: errorKindCannotCommunicate},
		{name: "connection string match", err: errors.New("lost connection to server"), want: errorKindCannotCommunicate},
		{name: "not found", err: mongo.ErrNoDocuments, want: errorKindObjectNotFound},
		{name: "duplicate key", err: mongo.WriteException{WriteErrors: []mongo.WriteError{{Code: 11000, Message: "duplicate key"}}}, want: errorKindConstraint},
		{name: "invalid regex type", err: mongo.CommandError{Code: 2, Message: "$regex has to be a string"}, want: errorKindInvalidQuery},
		{name: "invalid regex value", err: mongo.CommandError{Code: 51091, Message: "regular expression is invalid"}, want: errorKindInvalidQuery},
		{name: "command communication code", err: mongo.CommandError{Code: 91, Message: "ShutdownInProgress"}, want: errorKindCannotCommunicate},
		{name: "write concern communication code", err: mongo.WriteException{WriteConcernError: &mongo.WriteConcernError{Code: 91, Message: "ShutdownInProgress"}}, want: errorKindCannotCommunicate},
		{name: "bulk write communication code", err: mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{{WriteError: mongo.WriteError{Code: 91, Message: "ShutdownInProgress"}}}}, want: errorKindCannotCommunicate},
		{name: "generic error", err: errors.New("boom"), want: errorKindCannotExecuteQuery},
	}

	for _, mapper := range mappers {
		for _, tt := range tests {
			t.Run(mapper.name+"/"+tt.name, func(t *testing.T) {
				assertErrorKind(t, mapper.fn(tt.err), tt.want)
			})
		}
	}
}

func TestRunQueryRetryBehavior(t *testing.T) {
	t.Run("RunQuery/plain_context_deadline_does_not_retry", func(t *testing.T) {
		retryCalls := 0
		mctx := manipulate.NewContext(
			context.Background(),
			manipulate.ContextOptionRetryFunc(func(manipulate.RetryInfo) error {
				retryCalls++
				return nil
			}),
		)

		_, err := RunQuery(mctx, func() (any, error) {
			return nil, context.DeadlineExceeded
		}, RetryInfo{})
		assertErrorKind(t, err, errorKindCannotExecuteQuery)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected returned error to wrap context.DeadlineExceeded, got %v", err)
		}
		if retryCalls != 0 {
			t.Fatalf("expected retry func not to be called, got %d", retryCalls)
		}
	})

	t.Run("RunQuery/plain_context_cancel_does_not_retry", func(t *testing.T) {
		retryCalls := 0
		mctx := manipulate.NewContext(
			context.Background(),
			manipulate.ContextOptionRetryFunc(func(manipulate.RetryInfo) error {
				retryCalls++
				return nil
			}),
		)

		_, err := RunQuery(mctx, func() (any, error) {
			return nil, context.Canceled
		}, RetryInfo{})
		assertErrorKind(t, err, errorKindCannotExecuteQuery)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected returned error to wrap context.Canceled, got %v", err)
		}
		if retryCalls != 0 {
			t.Fatalf("expected retry func not to be called, got %d", retryCalls)
		}
	})

	t.Run("runQueryMongo/retryable_timeout_uses_retry_func", func(t *testing.T) {
		retryErr := errors.New("stop after first retry")
		retryCalls := 0
		mctx := manipulate.NewContext(
			context.Background(),
			manipulate.ContextOptionRetryFunc(func(info manipulate.RetryInfo) error {
				retryCalls++
				if info.Try() != 0 {
					t.Fatalf("unexpected try number: %d", info.Try())
				}
				var cannotCommunicate manipulate.ErrCannotCommunicate
				if !errors.As(info.Err(), &cannotCommunicate) {
					t.Fatalf("expected mapped retry error, got %T: %v", info.Err(), info.Err())
				}
				return retryErr
			}),
		)

		_, err := runQueryMongo(mctx, func() (any, error) {
			return nil, context.DeadlineExceeded
		}, RetryInfo{})
		if !errors.Is(err, retryErr) {
			t.Fatalf("expected retry func error %v, got %v", retryErr, err)
		}
		if retryCalls != 1 {
			t.Fatalf("expected retry func to be called once, got %d", retryCalls)
		}
	})

	runners := []struct {
		name string
		run  func(manipulate.Context, func() (any, error), RetryInfo) (any, error)
	}{
		{name: "RunQuery", run: RunQuery},
		{name: "runQueryMongo", run: runQueryMongo},
	}

	for _, runner := range runners {
		t.Run(runner.name+"/operation_context_timeout_does_not_retry", func(t *testing.T) {
			retryCalls := 0
			mctx := manipulate.NewContext(
				context.Background(),
				manipulate.ContextOptionRetryFunc(func(manipulate.RetryInfo) error {
					retryCalls++
					return nil
				}),
			)

			_, err := runner.run(mctx, func() (any, error) {
				return nil, &mongoOperationContextError{Err: context.DeadlineExceeded}
			}, RetryInfo{})
			assertErrorKind(t, err, errorKindCannotExecuteQuery)
			if retryCalls != 0 {
				t.Fatalf("expected retry func not to be called, got %d", retryCalls)
			}
		})

		t.Run(runner.name+"/non_retryable_error_does_not_retry", func(t *testing.T) {
			retryCalls := 0
			mctx := manipulate.NewContext(
				context.Background(),
				manipulate.ContextOptionRetryFunc(func(manipulate.RetryInfo) error {
					retryCalls++
					return nil
				}),
			)

			_, err := runner.run(mctx, func() (any, error) {
				return nil, mongo.WriteException{WriteErrors: []mongo.WriteError{{Code: 11000, Message: "duplicate key"}}}
			}, RetryInfo{})
			assertErrorKind(t, err, errorKindConstraint)
			if retryCalls != 0 {
				t.Fatalf("expected retry func not to be called, got %d", retryCalls)
			}
		})

		t.Run(runner.name+"/canceled_context_after_retryable_error_returns_cannot_execute", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			retryCalls := 0
			mctx := manipulate.NewContext(
				ctx,
				manipulate.ContextOptionRetryFunc(func(manipulate.RetryInfo) error {
					retryCalls++
					return nil
				}),
			)

			_, err := runner.run(mctx, func() (any, error) {
				return nil, timeoutNetError{message: "timeout"}
			}, RetryInfo{})
			assertErrorKind(t, err, errorKindCannotExecuteQuery)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("expected returned error to wrap context.Canceled, got %v", err)
			}
			if retryCalls != 1 {
				t.Fatalf("expected retry func to be called once, got %d", retryCalls)
			}
		})
	}
}

func TestMakeFieldsSelector(t *testing.T) {
	spec := &testAttributeSpec{fields: map[string]string{"name": "displayname"}}
	got := makeFieldsSelector([]string{"name"}, spec)
	want := mongobson.M{"displayname": 1}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected field selector with spec: got %v want %v", got, want)
	}

	got = makeFieldsSelector([]string{"ID"}, nil)
	want = mongobson.M{"_id": 1}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected field selector without spec: got %v want %v", got, want)
	}
}

func TestMongoIndexConflictError(t *testing.T) {
	if !isMongoIndexConflictError(mongo.CommandError{Code: 85, Message: "IndexOptionsConflict"}) {
		t.Fatalf("expected command error conflict to be detected")
	}
	if isMongoIndexConflictError(errors.New("some other error")) {
		t.Fatalf("unexpected non-conflict classification")
	}
}

func TestConvertReadPreferenceMongo(t *testing.T) {
	tests := []struct {
		name string
		in   manipulate.ReadConsistency
		mode string
	}{
		{name: "default", in: manipulate.ReadConsistencyDefault, mode: ""},
		{name: "eventual", in: manipulate.ReadConsistencyEventual, mode: "nearest"},
		{name: "monotonic", in: manipulate.ReadConsistencyMonotonic, mode: "primaryPreferred"},
		{name: "nearest", in: manipulate.ReadConsistencyNearest, mode: "nearest"},
		{name: "strong", in: manipulate.ReadConsistencyStrong, mode: "primary"},
		{name: "weakest", in: manipulate.ReadConsistencyWeakest, mode: "secondaryPreferred"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rp := convertReadPreferenceMongo(tt.in)
			if tt.mode == "" {
				if rp != nil {
					t.Fatalf("expected nil read preference, got %#v", rp)
				}
				return
			}
			if rp == nil {
				t.Fatalf("expected non-nil read preference")
			}
			if got := rp.Mode().String(); got != tt.mode {
				t.Fatalf("unexpected read preference mode: got %q want %q", got, tt.mode)
			}
		})
	}
}

func assertErrorKind(t *testing.T, err error, want errorKind) {
	t.Helper()

	switch want {
	case errorKindNil:
		if err != nil {
			t.Fatalf("expected nil error, got %T: %v", err, err)
		}
	case errorKindCannotCommunicate:
		var target manipulate.ErrCannotCommunicate
		if !errors.As(err, &target) {
			t.Fatalf("expected ErrCannotCommunicate, got %T: %v", err, err)
		}
	case errorKindCannotExecuteQuery:
		var target manipulate.ErrCannotExecuteQuery
		if !errors.As(err, &target) {
			t.Fatalf("expected ErrCannotExecuteQuery, got %T: %v", err, err)
		}
	case errorKindConstraint:
		var target manipulate.ErrConstraintViolation
		if !errors.As(err, &target) {
			t.Fatalf("expected ErrConstraintViolation, got %T: %v", err, err)
		}
	case errorKindInvalidQuery:
		var target manipulate.ErrInvalidQuery
		if !errors.As(err, &target) {
			t.Fatalf("expected ErrInvalidQuery, got %T: %v", err, err)
		}
		if !target.DueToFilter {
			t.Fatalf("expected invalid query to be marked DueToFilter")
		}
	case errorKindObjectNotFound:
		var target manipulate.ErrObjectNotFound
		if !errors.As(err, &target) {
			t.Fatalf("expected ErrObjectNotFound, got %T: %v", err, err)
		}
	default:
		t.Fatalf("unsupported error kind %q", want)
	}
}
