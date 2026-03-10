package manipmongo

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.acuvity.ai/elemental"
	"go.acuvity.ai/manipulate"
	bson "go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

func TestWorkerAHandleQueryErrorMappings(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		assert func(t *testing.T, got error)
	}{
		{
			name: "network_error",
			err:  &net.OpError{Op: "dial", Err: errors.New("boom")},
			assert: func(t *testing.T, got error) {
				t.Helper()
				if !manipulate.IsCannotCommunicateError(got) {
					t.Fatalf("expected ErrCannotCommunicate, got %T (%v)", got, got)
				}
			},
		},
		{
			name: "not_found",
			err:  mongo.ErrNoDocuments,
			assert: func(t *testing.T, got error) {
				t.Helper()
				var target manipulate.ErrObjectNotFound
				if !errors.As(got, &target) {
					t.Fatalf("expected ErrObjectNotFound, got %T (%v)", got, got)
				}
			},
		},
		{
			name: "duplicate_key",
			err:  mongo.WriteException{WriteErrors: []mongo.WriteError{{Code: 11000, Message: "dup"}}},
			assert: func(t *testing.T, got error) {
				t.Helper()
				var target manipulate.ErrConstraintViolation
				if !errors.As(got, &target) {
					t.Fatalf("expected ErrConstraintViolation, got %T (%v)", got, got)
				}
			},
		},
		{
			name: "invalid_query_regex",
			err:  mongo.CommandError{Code: 2, Message: errInvalidQueryBadRegex},
			assert: func(t *testing.T, got error) {
				t.Helper()
				var target manipulate.ErrInvalidQuery
				if !errors.As(got, &target) {
					t.Fatalf("expected ErrInvalidQuery, got %T (%v)", got, got)
				}
				if !target.DueToFilter {
					t.Fatalf("expected DueToFilter=true")
				}
			},
		},
		{
			name: "command_error_communication",
			err:  mongo.CommandError{Code: 6, Message: "host unreachable"},
			assert: func(t *testing.T, got error) {
				t.Helper()
				if !manipulate.IsCannotCommunicateError(got) {
					t.Fatalf("expected ErrCannotCommunicate, got %T (%v)", got, got)
				}
			},
		},
		{
			name: "generic_query_error",
			err:  errors.New("query failed"),
			assert: func(t *testing.T, got error) {
				t.Helper()
				var target manipulate.ErrCannotExecuteQuery
				if !errors.As(got, &target) {
					t.Fatalf("expected ErrCannotExecuteQuery, got %T (%v)", got, got)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := HandleQueryError(tc.err)
			tc.assert(t, got)
		})
	}
}

func TestWorkerAConnectionAndQueryErrorHelpers(t *testing.T) {
	if !isConnectionError(errors.New("lost connection to server")) {
		t.Fatalf("expected connection error to be detected")
	}
	if isConnectionError(errors.New("unrelated")) {
		t.Fatalf("unexpected connection error detection")
	}

	cmdErr := mongo.CommandError{Code: 42, Message: "boom"}
	if code := getErrorCode(cmdErr); code != 42 {
		t.Fatalf("unexpected error code: got %d want 42", code)
	}

	ok, invalid := invalidQuery(mongo.CommandError{Code: 2, Message: errInvalidQueryBadRegex})
	if !ok {
		t.Fatalf("expected invalid query to be detected")
	}
	var invalidQueryErr manipulate.ErrInvalidQuery
	if !errors.As(invalid, &invalidQueryErr) {
		t.Fatalf("expected manipulate.ErrInvalidQuery, got %T (%v)", invalid, invalid)
	}

	ok, invalid = invalidQuery(cmdErr)
	if ok || invalid != nil {
		t.Fatalf("expected non-invalid command error, got ok=%v invalid=%v", ok, invalid)
	}
}

func TestWorkerANamespaceAndUserFilters(t *testing.T) {
	nsCtx := manipulate.NewContext(
		context.Background(),
		manipulate.ContextOptionNamespace("tenant-a"),
		manipulate.ContextOptionRecursive(true),
		manipulate.ContextOptionPropagated(true),
	)
	if nsFilter := makeNamespaceFilter(nsCtx); len(nsFilter) == 0 {
		t.Fatalf("expected namespace filter")
	}

	spec := &compilerTestAttributeSpec{fields: map[string]string{"tenantid": "tid"}}
	userFilterExpr := elemental.NewFilterComposer().WithKey("tenantid").Equals("acuvity").Done()
	userCtx := manipulate.NewContext(context.Background(), manipulate.ContextOptionFilter(userFilterExpr))
	userFilter := makeUserFilter(userCtx, spec)
	if len(userFilter) == 0 {
		t.Fatalf("expected user filter")
	}

	encoded, err := bson.MarshalExtJSON(userFilter, false, false)
	if err != nil {
		t.Fatalf("unable to marshal user filter: %v", err)
	}
	if !strings.Contains(string(encoded), "\"tid\"") {
		t.Fatalf("expected translated key 'tid' in user filter, got %s", string(encoded))
	}
}

func TestWorkerAPipelineAndSelectorHelpers(t *testing.T) {
	id := bson.NewObjectID()
	retriever := func(bson.ObjectID) (bson.M, error) {
		return bson.M{"name": "alpha"}, nil
	}

	pipe, err := makePipeline(
		nil,
		retriever,
		bson.D{{Key: "tenant", Value: "acuvity"}},
		nil,
		nil,
		bson.D{{Key: "status", Value: bson.D{{Key: "$eq", Value: "active"}}}},
		[]string{"name"},
		id.Hex(),
		5,
		[]string{"id", "name"},
	)
	if err != nil {
		t.Fatalf("unexpected pipeline build error: %v", err)
	}
	if len(pipe) == 0 {
		t.Fatalf("expected non-empty pipeline")
	}
	if !pipelineContainsStage(pipe, "$sort") {
		t.Fatalf("expected $sort stage in pipeline: %#v", pipe)
	}
	if !pipelineContainsStage(pipe, "$project") {
		t.Fatalf("expected $project stage in pipeline: %#v", pipe)
	}

	_, err = makePipeline(nil, retriever, nil, nil, nil, nil, nil, "bad-after", 0, nil)
	if err == nil {
		t.Fatalf("expected invalid after id error")
	}
	var cannotExecute manipulate.ErrCannotExecuteQuery
	if !errors.As(err, &cannotExecute) {
		t.Fatalf("expected ErrCannotExecuteQuery, got %T (%v)", err, err)
	}

	spec := &compilerTestAttributeSpec{fields: map[string]string{"name": "n"}}
	sels := makeFieldsSelector([]string{"name", "id"}, spec)
	if sels["n"] != 1 || sels["id"] != 1 {
		t.Fatalf("unexpected selector map: %#v", sels)
	}
}

func TestWorkerAOrderingAndConsistencyConverters(t *testing.T) {
	spec := &compilerTestAttributeSpec{fields: map[string]string{"createdat": "created_at"}}
	order := applyOrdering([]string{"-createdat", "ID"}, spec)
	if len(order) != 2 || order[0] != "-created_at" || order[1] != "id" {
		t.Fatalf("unexpected ordering result: %#v", order)
	}

	if rp := convertReadConsistency(manipulate.ReadConsistencyEventual); rp == nil || rp.Mode() != readpref.SecondaryPreferredMode {
		t.Fatalf("unexpected eventual read preference: %#v", rp)
	}
	if rp := convertReadConsistency(manipulate.ReadConsistencyMonotonic); rp == nil || rp.Mode() != readpref.PrimaryPreferredMode {
		t.Fatalf("unexpected monotonic read preference: %#v", rp)
	}
	if rp := convertReadConsistency(manipulate.ReadConsistencyStrong); rp == nil || rp.Mode() != readpref.PrimaryMode {
		t.Fatalf("unexpected strong read preference: %#v", rp)
	}
	if rp := convertReadConsistency(manipulate.ReadConsistencyDefault); rp != nil {
		t.Fatalf("expected nil read preference for default, got %#v", rp)
	}

	if wc := convertWriteConsistency(manipulate.WriteConsistencyNone); wc == nil || wc.Acknowledged() {
		t.Fatalf("unexpected write concern for none: %#v", wc)
	}
	if wc := convertWriteConsistency(manipulate.WriteConsistencyStrong); wc == nil || wc.W != writeconcern.WCMajority {
		t.Fatalf("unexpected write concern for strong: %#v", wc)
	}
	if wc := convertWriteConsistency(manipulate.WriteConsistencyStrongest); wc == nil || wc.W != writeconcern.WCMajority || wc.Journal == nil || !*wc.Journal {
		t.Fatalf("unexpected write concern for strongest: %#v", wc)
	}
	if wc := convertWriteConsistency(manipulate.WriteConsistencyDefault); wc == nil || !wc.Acknowledged() {
		t.Fatalf("unexpected write concern for default: %#v", wc)
	}
}

type workerAMaxTimeQuery struct {
	last time.Duration
}

func (q workerAMaxTimeQuery) SetMaxTime(d time.Duration) workerAMaxTimeQuery {
	q.last = d
	return q
}

func TestWorkerASetMaxTime(t *testing.T) {
	q := workerAMaxTimeQuery{}
	updated, err := setMaxTime(context.Background(), q)
	if err != nil {
		t.Fatalf("unexpected setMaxTime error: %v", err)
	}
	if updated.last != defaultGlobalContextTimeout {
		t.Fatalf("unexpected default max time: got %s want %s", updated.last, defaultGlobalContextTimeout)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	cancel()
	if _, err := setMaxTime(ctx, q); err == nil {
		t.Fatalf("expected context cancellation error")
	}
}

func TestWorkerAExplainIfNeededSelection(t *testing.T) {
	identity := elemental.MakeIdentity("thing", "things")
	query := &workerAExplainableStub{}

	if callback := explainIfNeeded(query, nil, identity, elemental.OperationRetrieve, nil); callback != nil {
		t.Fatalf("expected nil callback when explain map is empty")
	}

	callback := explainIfNeeded(
		query,
		bson.D{{Key: "k", Value: "v"}},
		identity,
		elemental.OperationRetrieve,
		map[elemental.Identity]map[elemental.Operation]struct{}{
			identity: {elemental.OperationRetrieve: {}},
		},
	)
	if callback == nil {
		t.Fatalf("expected explain callback")
	}
	if err := callback(); err != nil {
		t.Fatalf("unexpected explain callback error: %v", err)
	}
}

func pipelineContainsStage(pipe []any, key string) bool {
	for _, stage := range pipe {
		doc, ok := stage.(bson.D)
		if !ok || len(doc) == 0 {
			continue
		}
		if doc[0].Key == key {
			return true
		}
	}
	return false
}

func TestWorkerAHandleQueryErrorFallsBackToExecute(t *testing.T) {
	baseErr := fmt.Errorf("unexpected")
	got := HandleQueryError(baseErr)
	var cannotExecute manipulate.ErrCannotExecuteQuery
	if !errors.As(got, &cannotExecute) {
		t.Fatalf("expected ErrCannotExecuteQuery for fallback path, got %T (%v)", got, got)
	}
}

func TestWorkerAMongoQueryErrorMethods(t *testing.T) {
	var nilErr *mongoQueryError
	if got := nilErr.Error(); got != "" {
		t.Fatalf("expected empty string on nil receiver, got %q", got)
	}
	if got := nilErr.Unwrap(); got != nil {
		t.Fatalf("expected nil unwrap on nil receiver, got %v", got)
	}

	inner := errors.New("inner")
	errWithInner := &mongoQueryError{Message: "outer", Err: inner}
	if got := errWithInner.Error(); got != "inner" {
		t.Fatalf("expected wrapped error message, got %q", got)
	}
	if got := errWithInner.Unwrap(); !errors.Is(got, inner) {
		t.Fatalf("expected unwrap to return wrapped error, got %v", got)
	}

	msgOnly := &mongoQueryError{Message: "message only"}
	if got := msgOnly.Error(); got != "message only" {
		t.Fatalf("expected message fallback, got %q", got)
	}
	if got := msgOnly.Unwrap(); got != nil {
		t.Fatalf("expected nil unwrap without wrapped error, got %v", got)
	}
}

func TestWorkerAQueryErrorBranches(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		expectOK     bool
		expectedCode int
		expectedMsg  string
	}{
		{name: "nil", err: nil, expectOK: false},
		{
			name:         "command_error",
			err:          mongo.CommandError{Code: 100, Message: "command failed"},
			expectOK:     true,
			expectedCode: 100,
			expectedMsg:  "command failed",
		},
		{
			name:         "write_error",
			err:          mongo.WriteError{Code: 101, Message: "write failed"},
			expectOK:     true,
			expectedCode: 101,
			expectedMsg:  "write failed",
		},
		{
			name: "write_exception_with_write_error",
			err: mongo.WriteException{
				WriteErrors: mongo.WriteErrors{{Code: 102, Message: "write exception write error"}},
			},
			expectOK:     true,
			expectedCode: 102,
			expectedMsg:  "write exception write error",
		},
		{
			name: "write_exception_with_write_concern_error",
			err: mongo.WriteException{
				WriteConcernError: &mongo.WriteConcernError{Code: 103, Message: "write concern failed"},
			},
			expectOK:     true,
			expectedCode: 103,
			expectedMsg:  "write concern failed",
		},
		{
			name:     "write_exception_without_details",
			err:      mongo.WriteException{},
			expectOK: false,
		},
		{
			name: "bulk_write_exception_with_write_error",
			err: mongo.BulkWriteException{
				WriteErrors: []mongo.BulkWriteError{{
					WriteError: mongo.WriteError{Code: 104, Message: "bulk write failed"},
				}},
			},
			expectOK:     true,
			expectedCode: 104,
			expectedMsg:  "bulk write failed",
		},
		{
			name: "bulk_write_exception_with_write_concern_error",
			err: mongo.BulkWriteException{
				WriteConcernError: &mongo.WriteConcernError{Code: 105, Message: "bulk write concern failed"},
			},
			expectOK:     true,
			expectedCode: 105,
			expectedMsg:  "bulk write concern failed",
		},
		{
			name:     "bulk_write_exception_without_details",
			err:      mongo.BulkWriteException{},
			expectOK: false,
		},
		{
			name:     "untyped_error",
			err:      errors.New("plain"),
			expectOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qErr, ok := queryError(tt.err)
			if ok != tt.expectOK {
				t.Fatalf("unexpected ok state: got %v want %v (err=%v)", ok, tt.expectOK, tt.err)
			}
			if !tt.expectOK {
				if qErr != nil {
					t.Fatalf("expected nil query error, got %#v", qErr)
				}
				return
			}
			if qErr == nil {
				t.Fatalf("expected non-nil query error")
			}
			if qErr.Code != tt.expectedCode || qErr.Message != tt.expectedMsg {
				t.Fatalf("unexpected query error mapping: got code=%d message=%q want code=%d message=%q",
					qErr.Code, qErr.Message, tt.expectedCode, tt.expectedMsg)
			}
		})
	}
}

type workerAObjectIDAlias bson.ObjectID
type workerAStringAlias string

func TestWorkerAConvertObjectIDForMethodBranches(t *testing.T) {
	id := bson.NewObjectID()

	idType := reflect.TypeOf(id)
	idValue, err := convertObjectIDForMethod(id, idType)
	if err != nil {
		t.Fatalf("unexpected assignable conversion error: %v", err)
	}
	if got, ok := idValue.Interface().(bson.ObjectID); !ok || got != id {
		t.Fatalf("unexpected assignable conversion result: %#v", idValue.Interface())
	}

	var aliasValue workerAObjectIDAlias
	aliasType := reflect.TypeOf(aliasValue)
	convertedAlias, err := convertObjectIDForMethod(id, aliasType)
	if err != nil {
		t.Fatalf("unexpected convertible object id conversion error: %v", err)
	}
	gotAlias, ok := convertedAlias.Interface().(workerAObjectIDAlias)
	if !ok || bson.ObjectID(gotAlias) != id {
		t.Fatalf("unexpected alias conversion result: %#v", convertedAlias.Interface())
	}

	stringType := reflect.TypeOf("")
	hexValue, err := convertObjectIDForMethod(id, stringType)
	if err != nil {
		t.Fatalf("unexpected string conversion error: %v", err)
	}
	if got := hexValue.Interface().(string); got != id.Hex() {
		t.Fatalf("unexpected hex conversion result: got %q want %q", got, id.Hex())
	}

	var aliasStringValue workerAStringAlias
	aliasStringType := reflect.TypeOf(aliasStringValue)
	convertedStringAlias, err := convertObjectIDForMethod(id, aliasStringType)
	if err != nil {
		t.Fatalf("unexpected convertible string conversion error: %v", err)
	}
	if got := convertedStringAlias.Interface().(workerAStringAlias); got != workerAStringAlias(id.Hex()) {
		t.Fatalf("unexpected alias string conversion result: got %q want %q", got, workerAStringAlias(id.Hex()))
	}

	if _, err := convertObjectIDForMethod(id, reflect.TypeOf(0)); err == nil {
		t.Fatalf("expected conversion error for incompatible target type")
	}
}

type workerAMissingFindIDCollection struct{}

type workerABadFindIDSignatureCollection struct{}

func (workerABadFindIDSignatureCollection) FindId() *workerAFakeQuery {
	return &workerAFakeQuery{}
}

type workerABadFindIDResultArityCollection struct{}

func (workerABadFindIDResultArityCollection) FindId(string) (*workerAFakeQuery, error) {
	return &workerAFakeQuery{}, nil
}

type workerAQueryWithoutOne struct{}

type workerAMissingOneMethodCollection struct{}

func (workerAMissingOneMethodCollection) FindId(string) workerAQueryWithoutOne {
	return workerAQueryWithoutOne{}
}

type workerABadOneResultArityQuery struct{}

func (workerABadOneResultArityQuery) One(any) (error, error) {
	return nil, nil
}

type workerABadOneResultArityCollection struct{}

func (workerABadOneResultArityCollection) FindId(string) workerABadOneResultArityQuery {
	return workerABadOneResultArityQuery{}
}

type workerANonErrorOneResultQuery struct{}

func (workerANonErrorOneResultQuery) One(any) any {
	return 123
}

type workerANonErrorOneResultCollection struct{}

func (workerANonErrorOneResultCollection) FindId(string) workerANonErrorOneResultQuery {
	return workerANonErrorOneResultQuery{}
}

type workerAErrorOneResultQuery struct{}

func (workerAErrorOneResultQuery) One(any) error {
	return errors.New("one failed")
}

type workerAErrorOneResultCollection struct{}

func (workerAErrorOneResultCollection) FindId(string) workerAErrorOneResultQuery {
	return workerAErrorOneResultQuery{}
}

type workerAIncompatibleFindIDArgCollection struct{}

func (workerAIncompatibleFindIDArgCollection) FindId(bool) *workerAFakeQuery {
	return &workerAFakeQuery{}
}

func TestWorkerAMakePreviousRetrieverByReflectionErrorBranches(t *testing.T) {
	id := bson.NewObjectID()

	tests := []struct {
		name       string
		collection any
		wantErr    string
	}{
		{
			name:       "missing_find_id_method",
			collection: workerAMissingFindIDCollection{},
			wantErr:    "missing FindId method",
		},
		{
			name:       "unsupported_find_id_signature",
			collection: workerABadFindIDSignatureCollection{},
			wantErr:    "unsupported FindId signature",
		},
		{
			name:       "incompatible_find_id_argument",
			collection: workerAIncompatibleFindIDArgCollection{},
			wantErr:    "cannot convert mongo object id",
		},
		{
			name:       "unsupported_find_id_result_arity",
			collection: workerABadFindIDResultArityCollection{},
			wantErr:    "unsupported FindId result arity",
		},
		{
			name:       "missing_one_method",
			collection: workerAMissingOneMethodCollection{},
			wantErr:    "missing One method",
		},
		{
			name:       "unsupported_one_result_arity",
			collection: workerABadOneResultArityCollection{},
			wantErr:    "unsupported One result arity",
		},
		{
			name:       "one_returns_non_error_type",
			collection: workerANonErrorOneResultCollection{},
			wantErr:    "query failed with non-error type",
		},
		{
			name:       "one_returns_error_type",
			collection: workerAErrorOneResultCollection{},
			wantErr:    "one failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := makePreviousRetrieverByReflection(tt.collection, id)
			if err == nil {
				t.Fatalf("expected error for %s", tt.name)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("unexpected error: got %q want fragment %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestWorkerAApplyOrderingSpecialCases(t *testing.T) {
	got := applyOrdering([]string{"", "ID", "-id", "Name"}, nil)
	want := []string{"_id", "-_id", "name"}
	if len(got) != len(want) {
		t.Fatalf("unexpected ordering length: got %#v want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected ordering at index %d: got %q want %q", i, got[i], want[i])
		}
	}
}

func TestWorkerAMakeFieldsSelectorEmptyBranches(t *testing.T) {
	if got := makeFieldsSelector(nil, nil); got != nil {
		t.Fatalf("expected nil selector for empty fields, got %#v", got)
	}
	if got := makeFieldsSelector([]string{""}, nil); got != nil {
		t.Fatalf("expected nil selector when all fields are empty, got %#v", got)
	}
}
