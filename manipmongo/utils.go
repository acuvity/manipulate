// Copyright 2019 Aporeto Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package manipmongo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	neturl "net/url"
	"strings"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
	"go.acuvity.ai/elemental"
	"go.acuvity.ai/manipulate"
	"go.acuvity.ai/manipulate/internal/backoff"
	bson "go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

const (
	errInvalidQueryInvalidRegex = "regular expression is invalid"
	errInvalidQueryBadRegex     = "$regex has to be a string"
)

type countRes struct {
	Count int `bson:"_count"`
}

type mongoOperationContextError struct {
	Err error
}

type mongoQueryError struct {
	Code    int
	Message string
	Err     error
}

func (e *mongoOperationContextError) Error() string {
	if e == nil || e.Err == nil {
		return ""
	}
	return e.Err.Error()
}

func (e *mongoOperationContextError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func (e *mongoQueryError) Error() string {
	if e == nil {
		return ""
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return e.Message
}

func (e *mongoQueryError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func HandleQueryErrorMongo(err error) error {
	return HandleQueryError(err)
}

// HandleQueryError handles the provided upstream error returned by Mongo by returning a corresponding manipulate error type.
func HandleQueryError(err error) error {
	if err == nil {
		return nil
	}

	var operationCtxErr *mongoOperationContextError
	if errors.As(err, &operationCtxErr) {
		return manipulate.ErrCannotExecuteQuery{Err: operationCtxErr.Err}
	}

	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return manipulate.ErrCannotCommunicate{Err: err}
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return manipulate.ErrCannotCommunicate{Err: err}
	}

	if errors.Is(err, mongo.ErrNoDocuments) {
		return manipulate.ErrObjectNotFound{Err: fmt.Errorf("cannot find the object for the given ID")}
	}

	if mongo.IsDuplicateKeyError(err) {
		return manipulate.ErrConstraintViolation{Err: fmt.Errorf("duplicate key")}
	}

	if mongo.IsTimeout(err) || mongo.IsNetworkError(err) || isConnectionError(err) {
		return manipulate.ErrCannotCommunicate{Err: err}
	}

	if ok, invalidErr := invalidQuery(err); ok {
		return invalidErr
	}

	// see https://github.com/mongodb/mongo/blob/master/src/mongo/base/error_codes.err
	switch getErrorCode(err) {
	case 6, 7, 71, 74, 91, 109, 189, 202, 216, 262, 10107, 13436, 13435, 11600, 11602:
		// HostUnreachable
		// HostNotFound
		// ReplicaSetNotFound
		// NodeNotFound
		// ConfigurationInProgress
		// ShutdownInProgress
		// PrimarySteppedDown
		// NetworkInterfaceExceededTimeLimit
		// ElectionInProgress
		// ExceededTimeLimit
		// NotMaster
		// NotMasterOrSecondary
		// NotMasterNoSlaveOk
		// InterruptedAtShutdown
		// InterruptedDueToStepDown
		return manipulate.ErrCannotCommunicate{Err: err}
	default:
		return manipulate.ErrCannotExecuteQuery{Err: err}
	}
}

func applyOrdering(order []string, spec elemental.AttributeSpecifiable) []string {
	o := []string{} // nolint: prealloc

	for _, f := range order {
		if f == "" {
			continue
		}

		if spec != nil {
			trimmed := strings.TrimPrefix(f, "-")
			if attrSpec := spec.SpecificationForAttribute(trimmed); attrSpec.BSONFieldName != "" {
				original := f
				f = attrSpec.BSONFieldName
				// if we stripped the "-" from the field name, we add it back to the BSON representation of the field name.
				if trimmed != original {
					f = "-" + f
				}
			}
		} else {
			if f == "ID" || f == "id" {
				f = "_id"
			}

			if f == "-ID" || f == "-id" {
				f = "-_id"
			}
		}

		o = append(o, strings.ToLower(f))
	}

	return o
}

func redactMongoURI(raw string) string {
	parsed, err := neturl.Parse(raw)
	if err != nil || parsed == nil || parsed.User == nil {
		return raw
	}

	username := parsed.User.Username()
	if username == "" {
		username = "redacted"
	}

	if _, hasPassword := parsed.User.Password(); hasPassword {
		parsed.User = neturl.UserPassword(username, "REDACTED")
		return parsed.String()
	}

	parsed.User = neturl.User(username)
	return parsed.String()
}

func countFromResults(res []*countRes) (int, error) {
	switch len(res) {
	case 0:
		return 0, nil
	case 1:
		return res[0].Count, nil
	default:
		return 0, manipulate.ErrCannotExecuteQuery{Err: fmt.Errorf("count: invalid count result len: %d. must be 1", len(res))}
	}
}

// contextWithDefaultTimeout preserves the long-standing manipmongo behavior
// that a zero timeout means "use the package default" rather than "no
// timeout".
func contextWithDefaultTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, effectiveTimeout(timeout))
}

func effectiveTimeout(timeout time.Duration) time.Duration {
	if timeout > 0 {
		return timeout
	}
	return defaultOperationTimeout
}

func maxTimeFromContext(ctx context.Context, defaultTimeout time.Duration) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, manipulate.ErrCannotBuildQuery{Err: err}
	}

	d, ok := ctx.Deadline()
	if !ok {
		return effectiveTimeout(defaultTimeout), nil
	}
	return time.Until(d), nil
}

func mongoOperationContext(ctx context.Context, defaultTimeout time.Duration) (context.Context, context.CancelFunc, error) {
	maxTime, err := maxTimeFromContext(ctx, defaultTimeout)
	if err != nil {
		return nil, nil, err
	}
	if maxTime <= 0 {
		maxTime = time.Nanosecond
	}
	queryCtx, cancel := context.WithTimeout(ctx, maxTime)
	return queryCtx, cancel, nil
}

func wrapMongoOperationContextError(parentCtx context.Context, queryCtx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)) && queryCtx != nil && queryCtx.Err() != nil && parentCtx != nil && parentCtx.Err() == nil {
		return &mongoOperationContextError{Err: queryCtx.Err()}
	}
	return err
}

func identifierFilterElement(identifier string) bson.E {
	if oid, err := bson.ObjectIDFromHex(identifier); err == nil {
		return bson.E{Key: "_id", Value: oid}
	}
	return bson.E{Key: "_id", Value: identifier}
}

func composeAndFilter(filter bson.D, andFilters ...bson.D) bson.D {
	if len(andFilters) == 0 {
		return filter
	}

	clauses := make([]bson.D, 0, len(andFilters)+1)
	for _, clause := range andFilters {
		if len(clause) == 0 {
			continue
		}
		clauses = append(clauses, clause)
	}
	clauses = append(clauses, filter)

	return bson.D{{Key: "$and", Value: clauses}}
}

func convertUpsertOperationsToMongo(operations any) (bson.M, error) {
	switch typed := operations.(type) {
	case bson.M:
		if typed == nil {
			return bson.M{}, nil
		}
		return typed, nil
	default:
		return nil, fmt.Errorf("upsert operations must be bson.M")
	}
}

func mergeSetOnInsertOperationsMongo(dst bson.M, soi any) error {
	converted, err := convertSetOnInsertToMongoMap(soi)
	if err != nil {
		return err
	}
	maps.Copy(dst, converted)
	return nil
}

func convertSetOnInsertToMongoMap(soi any) (bson.M, error) {
	switch typed := soi.(type) {
	case bson.M:
		if typed == nil {
			return bson.M{}, nil
		}
		return typed, nil
	default:
		return nil, fmt.Errorf("$setOnInsert operations must be bson.M")
	}
}

func convertReadPreferenceMongo(c manipulate.ReadConsistency) *readpref.ReadPref {
	switch c {
	case manipulate.ReadConsistencyEventual:
		// mgo.Eventual is effectively Nearest, but without session stickiness
		// across sequential reads. The official driver does not expose that
		// stickiness behavior as a read preference, so Nearest is the closest
		// semantic match.
		return readpref.Nearest()
	case manipulate.ReadConsistencyMonotonic:
		// Monotonic has no exact equivalent in mongo-driver read preferences.
		// PrimaryPreferred is the safest approximation for read-after-write behavior.
		return readpref.PrimaryPreferred()
	case manipulate.ReadConsistencyNearest:
		return readpref.Nearest()
	case manipulate.ReadConsistencyStrong:
		return readpref.Primary()
	case manipulate.ReadConsistencyWeakest:
		return readpref.SecondaryPreferred()
	default:
		return nil
	}
}

func convertWriteConcernMongo(c manipulate.WriteConsistency) *writeconcern.WriteConcern {
	switch c {
	case manipulate.WriteConsistencyNone:
		return writeconcern.Unacknowledged()
	case manipulate.WriteConsistencyStrong:
		return writeconcern.Majority()
	case manipulate.WriteConsistencyStrongest:
		journaled := writeconcern.Journaled()
		journaled.W = writeconcern.WCMajority
		return journaled
	default:
		return writeconcern.W1()
	}
}

func runQueryMongo(mctx manipulate.Context, operationFunc func() (any, error), baseRetryInfo RetryInfo) (any, error) {
	var try int
	for {
		out, err := operationFunc()
		if err == nil {
			return out, nil
		}

		err = HandleQueryErrorMongo(err)
		if !manipulate.IsCannotCommunicateError(err) {
			return out, err
		}

		baseRetryInfo.try = try
		baseRetryInfo.err = err
		baseRetryInfo.mctx = mctx

		if rf := mctx.RetryFunc(); rf != nil {
			if rerr := rf(baseRetryInfo); rerr != nil {
				return nil, rerr
			}
		} else if baseRetryInfo.defaultRetryFunc != nil {
			if rerr := baseRetryInfo.defaultRetryFunc(baseRetryInfo); rerr != nil {
				return nil, rerr
			}
		}

		select {
		case <-mctx.Context().Done():
			return nil, manipulate.ErrCannotExecuteQuery{Err: mctx.Context().Err()}
		default:
		}

		deadline, _ := mctx.Context().Deadline()
		time.Sleep(backoff.NextWithCurve(try, deadline, defaultBackoffCurve))
		try++
	}
}

func makePreviousRetriever(ctx context.Context, coll *mongo.Collection, defaultTimeout time.Duration) func(id bson.ObjectID) (bson.M, error) {
	return func(id bson.ObjectID) (bson.M, error) {
		queryCtx, cancel, err := mongoOperationContext(ctx, defaultTimeout)
		if err != nil {
			return nil, err
		}
		defer cancel()

		doc := bson.M{}
		if err := coll.FindOne(queryCtx, bson.D{{Key: "_id", Value: id}}).Decode(&doc); err != nil {
			return nil, wrapMongoOperationContextError(ctx, queryCtx, err)
		}
		return doc, nil
	}
}

func makeShardingManyFilter(m *mongoManipulator, mctx manipulate.Context, identity elemental.Identity) (bson.D, error) {
	if m.sharder == nil {
		return nil, nil
	}

	filter, err := m.sharder.FilterMany(m, mctx, identity)
	if err != nil {
		return nil, manipulate.ErrCannotBuildQuery{Err: fmt.Errorf("cannot compute sharding filter: %w", err)}
	}

	return filter, nil
}

func makeShardingOneFilter(m *mongoManipulator, mctx manipulate.Context, object elemental.Identifiable) (bson.D, error) {
	if m.sharder == nil {
		return nil, nil
	}

	filter, err := m.sharder.FilterOne(m, mctx, object)
	if err != nil {
		return nil, manipulate.ErrCannotBuildQuery{Err: fmt.Errorf("cannot compute sharding filter: %w", err)}
	}

	return filter, nil
}

func makeNamespaceFilter(mctx manipulate.Context) bson.D {

	if mctx.Namespace() == "" {
		return nil
	}

	f := manipulate.NewNamespaceFilter(mctx.Namespace(), mctx.Recursive())
	if mctx.Propagated() {
		if fp := manipulate.NewPropagationFilter(mctx.Namespace()); fp != nil {
			f = elemental.NewFilterComposer().Or(f, fp).Done()
		}
	}

	return CompileFilter(f)
}

func makeUserFilter(mctx manipulate.Context, attrSpec elemental.AttributeSpecifiable) bson.D {

	f := mctx.Filter()

	if f == nil {
		return nil
	}

	var opts []CompilerOption
	if attrSpec != nil {
		opts = append(opts, CompilerOptionTranslateKeysFromSpec(attrSpec))
	}

	return CompileFilter(f, opts...)
}

func makePipeline(
	attrSpec elemental.AttributeSpecifiable,
	retriever func(id bson.ObjectID) (bson.M, error),
	shardFilter bson.D,
	namespaceFilter bson.D,
	forcedReadFilter bson.D,
	userFilter bson.D,
	order []string,
	after string,
	limit int,
	fields []string,
) (mongo.Pipeline, error) {

	pipe := mongo.Pipeline{}

	// Add sharding match.
	if len(shardFilter) > 0 {
		pipe = append(pipe, bson.D{{Key: "$match", Value: shardFilter}})
	}

	// Add namespace match.
	if len(namespaceFilter) > 0 {
		pipe = append(pipe, bson.D{{Key: "$match", Value: namespaceFilter}})
	}

	// Add forced match.
	if len(forcedReadFilter) > 0 {
		pipe = append(pipe, bson.D{{Key: "$match", Value: forcedReadFilter}})
	}

	// Ordering.
	if len(order) == 0 && after != "" {
		order = []string{"_id"}
	}

	if len(order) > 0 {

		id := bson.NilObjectID
		doc := bson.M{}
		match := []bson.D{}
		sort := bson.D{}
		hasID := false

		// If we have an after, we get the previous object info.
		if after != "" {

			oid, err := bson.ObjectIDFromHex(after)
			if err != nil {
				return nil, HandleQueryError(fmt.Errorf("after '%s' is not parsable objectId", after))
			}
			id = oid

			doc, err = retriever(id)
			if err != nil {
				return nil, HandleQueryError(fmt.Errorf("unable to retrieve previous object with after id '%s': %w", after, err))
			}
			if doc == nil {
				return nil, HandleQueryError(fmt.Errorf("unable to retrieve previous object with after id '%s': not found", after))
			}
		}

		// Now we range over the order fields.
		for _, f := range order {

			cmp, op := 1, "$gt"
			if strings.HasPrefix(f, "-") {
				cmp, op, f = -1, "$lt", strings.TrimPrefix(f, "-")
			}

			hasID = hasID || f == "_id"
			sort = append(sort, bson.E{Key: f, Value: cmp})

			if after != "" {
				if f == "_id" {
					match = append(match, bson.D{{Key: "_id", Value: bson.D{{Key: "$gt", Value: id}}}})
				} else {
					match = append(match,
						bson.D{{
							Key: "$or",
							Value: []bson.D{
								{{Key: f, Value: bson.D{{Key: op, Value: doc[f]}}}},
								{{Key: f, Value: doc[f]}, {Key: "_id", Value: bson.D{{Key: "$gt", Value: id}}}},
							},
						}},
					)
				}
			}
		}

		if !hasID {
			sort = append(sort, bson.E{Key: "_id", Value: 1})
		}

		pipe = append(pipe, bson.D{{Key: "$sort", Value: sort}})

		if after != "" {
			pipe = append(pipe, bson.D{{Key: "$match", Value: bson.D{{Key: "$and", Value: match}}}})
		}
	}

	// User filtering.
	if len(userFilter) > 0 {
		pipe = append(pipe, bson.D{{Key: "$match", Value: userFilter}})
	}

	// Limiting.
	if limit > 0 {
		pipe = append(pipe, bson.D{{Key: "$limit", Value: limit}})
	}

	// Fields.
	if sels := makeFieldsSelector(fields, attrSpec); sels != nil {
		pipe = append(pipe, bson.D{{Key: "$project", Value: sels}})
	}

	return pipe, nil
}

func spanErr(sp opentracing.Span, err error) error {

	if sp != nil {
		sp.SetTag("error", true)
		sp.LogFields(log.Error(err))
	}

	return err
}

func getErrorCode(err error) int {
	if qErr, ok := queryError(err); ok {
		return qErr.Code
	}
	return 0
}

func invalidQuery(err error) (bool, error) {

	qErr, ok := queryError(err)
	if !ok {
		return false, nil
	}

	errCopyLower := strings.ToLower(qErr.Message)
	switch {
	case qErr.Code == 2 && strings.Contains(errCopyLower, errInvalidQueryBadRegex):
		return true, manipulate.ErrInvalidQuery{
			DueToFilter: true,
			Err:         qErr,
		}
	case qErr.Code == 51091 && strings.Contains(errCopyLower, errInvalidQueryInvalidRegex):
		return true, manipulate.ErrInvalidQuery{
			DueToFilter: true,
			Err:         qErr,
		}
	default:
		return false, nil
	}
}

func queryError(err error) (*mongoQueryError, bool) {

	if err == nil {
		return nil, false
	}

	var commandErr mongo.CommandError
	if errors.As(err, &commandErr) {
		return &mongoQueryError{Code: int(commandErr.Code), Message: commandErr.Message, Err: commandErr}, true
	}

	var writeErr mongo.WriteError
	if errors.As(err, &writeErr) {
		return &mongoQueryError{Code: writeErr.Code, Message: writeErr.Message, Err: writeErr}, true
	}

	var writeException mongo.WriteException
	if errors.As(err, &writeException) {
		if len(writeException.WriteErrors) > 0 {
			we := writeException.WriteErrors[0]
			return &mongoQueryError{Code: we.Code, Message: we.Message, Err: we}, true
		}
		if writeException.WriteConcernError != nil {
			wce := writeException.WriteConcernError
			return &mongoQueryError{Code: wce.Code, Message: wce.Message, Err: wce}, true
		}
	}

	var bulkWriteException mongo.BulkWriteException
	if errors.As(err, &bulkWriteException) {
		if len(bulkWriteException.WriteErrors) > 0 {
			we := bulkWriteException.WriteErrors[0]
			return &mongoQueryError{Code: we.Code, Message: we.Message, Err: we}, true
		}
		if bulkWriteException.WriteConcernError != nil {
			wce := bulkWriteException.WriteConcernError
			return &mongoQueryError{Code: wce.Code, Message: wce.Message, Err: wce}, true
		}
	}

	return nil, false
}

func isConnectionError(err error) bool {

	if err == nil {
		return false
	}

	if mongo.IsNetworkError(err) {
		return true
	}

	// Stolen from mongodb code. this is ugly.
	const (
		errLostConnection               = "lost connection to server"
		errNoReachableServers           = "no reachable servers"
		errReplTimeoutPrefix            = "waiting for replication timed out"
		errCouldNotContactPrimaryPrefix = "could not contact primary for replica set"
		errWriteResultsUnavailable      = "write results unavailable from"
		errCouldNotFindPrimaryPrefix    = `could not find host matching read preference { mode: "primary"`
		errUnableToTargetPrefix         = "unable to target"
		errNotMaster                    = "not master"
		errConnectionRefusedSuffix      = "connection refused"
	)

	lowerCaseError := strings.ToLower(err.Error())
	if lowerCaseError == errNoReachableServers ||
		errors.Is(err, io.EOF) ||
		strings.Contains(lowerCaseError, errLostConnection) ||
		strings.Contains(lowerCaseError, errReplTimeoutPrefix) ||
		strings.Contains(lowerCaseError, errCouldNotContactPrimaryPrefix) ||
		strings.Contains(lowerCaseError, errWriteResultsUnavailable) ||
		strings.Contains(lowerCaseError, errCouldNotFindPrimaryPrefix) ||
		strings.Contains(lowerCaseError, errUnableToTargetPrefix) ||
		lowerCaseError == errNotMaster ||
		strings.HasSuffix(lowerCaseError, errConnectionRefusedSuffix) {
		return true
	}
	return false
}

func makeFieldsSelector(fields []string, spec elemental.AttributeSpecifiable) bson.M {

	if len(fields) == 0 {
		return nil
	}

	sels := bson.M{}
	for _, f := range fields {

		if f == "" {
			continue
		}

		f = strings.ToLower(strings.TrimPrefix(f, "-"))
		if spec != nil {
			// if a spec has been provided, use it to look up the BSON field name if there is an entry for the attribute.
			// if no entry was found for the attribute in the provided spec default to whatever value was provided for
			// the attribute.
			if as := spec.SpecificationForAttribute(f); as.BSONFieldName != "" {
				f = as.BSONFieldName
			}
		} else {
			if f == "id" {
				f = "_id"
			}
		}

		sels[f] = 1
	}

	if len(sels) == 0 {
		return nil
	}

	return sels
}

func isMongoIndexConflictError(err error) bool {
	if err == nil {
		return false
	}

	var cmdErr mongo.CommandError
	if errors.As(err, &cmdErr) {
		switch cmdErr.Code {
		case 85, 86:
			return true
		}
	}

	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "already exists with different options") ||
		strings.Contains(lower, "indexoptionsconflict") ||
		strings.Contains(lower, "indexkeyspecsconflict") ||
		strings.Contains(lower, "is reserved for the _id index") ||
		strings.Contains(lower, "for an _id index specification")
}

func shouldExplain(identity elemental.Identity, operation elemental.Operation, explainMap map[elemental.Identity]map[elemental.Operation]struct{}) bool {
	if len(explainMap) == 0 {
		return false
	}

	exp, ok := explainMap[identity]
	if !ok {
		return false
	}

	if len(exp) == 0 {
		return true
	}

	_, ok = exp[operation]
	return ok
}

func explainMongo(ctx context.Context, defaultTimeout time.Duration, db *mongo.Database, command bson.D, operation elemental.Operation, identity elemental.Identity, details any) error {
	queryCtx, cancel, err := mongoOperationContext(ctx, defaultTimeout)
	if err != nil {
		return err
	}
	defer cancel()

	result := bson.M{}
	if err := db.RunCommand(queryCtx, bson.D{{Key: "explain", Value: command}}).Decode(&result); err != nil {
		return fmt.Errorf("unable to explain: %w", err)
	}

	detailsString := "<none>"
	if details != nil {
		data, err := bson.MarshalExtJSONIndent(details, false, false, "", "  ")
		if err != nil {
			return fmt.Errorf("unable to marshal explanation details: %w", err)
		}
		detailsString = string(data)
	}

	resultData, err := bson.MarshalExtJSONIndent(result, false, false, "", "  ")
	if err != nil {
		return fmt.Errorf("unable to marshal explanation: %w", err)
	}

	fmt.Println("")
	fmt.Println("--------------------------------")
	fmt.Printf("Operation:   %s\n", operation)
	fmt.Printf("Identity:    %s\n", identity.Name)
	fmt.Printf("Details:     %s\n", detailsString)
	fmt.Println("Explanation:")
	fmt.Println(string(resultData))
	fmt.Println("--------------------------------")
	fmt.Println("")

	return nil
}
