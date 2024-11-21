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
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
	"go.acuvity.ai/elemental"
	"go.acuvity.ai/manipulate"
	"go.acuvity.ai/manipulate/internal/objectid"
)

const (
	descendingOrderPrefix       = "-"
	errInvalidQueryInvalidRegex = "regular expression is invalid"
	errInvalidQueryBadRegex     = "$regex has to be a string"
)

func applyOrdering(order []string, spec elemental.AttributeSpecifiable) []string {

	o := []string{} // nolint: prealloc

	for _, f := range order {

		if f == "" {
			continue
		}

		if spec != nil {
			trimmed := strings.TrimPrefix(f, descendingOrderPrefix)
			if attrSpec := spec.SpecificationForAttribute(trimmed); attrSpec.BSONFieldName != "" {
				original := f
				f = attrSpec.BSONFieldName
				// if we stripped the "-" from the field name, we add it back to the BSON representation of the field name.
				if trimmed != original {
					f = fmt.Sprintf("%s%s", descendingOrderPrefix, f)
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

func makePreviousRetriever(c *mgo.Collection) func(id bson.ObjectId) (bson.M, error) {

	return func(id bson.ObjectId) (bson.M, error) {
		doc := bson.M{}
		if err := c.FindId(id).One(&doc); err != nil {
			return nil, err
		}
		return doc, nil
	}
}

func makeShardingManyFilter(m *mongoManipulator, mctx manipulate.Context, identity elemental.Identity) (bson.D, error) {

	if m.sharder == nil {
		return nil, nil
	}

	sq, err := m.sharder.FilterMany(m, mctx, identity)
	if err != nil {
		return nil, manipulate.ErrCannotBuildQuery{Err: fmt.Errorf("cannot compute sharding filter: %w", err)}
	}

	return sq, nil
}

func makeShardingOneFilter(m *mongoManipulator, mctx manipulate.Context, object elemental.Identifiable) (bson.D, error) {

	if m.sharder == nil {
		return nil, nil
	}

	sq, err := m.sharder.FilterOne(m, mctx, object)
	if err != nil {
		return nil, manipulate.ErrCannotBuildQuery{Err: fmt.Errorf("cannot compute sharding filter: %w", err)}
	}

	return sq, nil
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
	retriever func(id bson.ObjectId) (bson.M, error),
	shardFilter bson.D,
	namespaceFiler bson.D,
	forcedReadFilter bson.D,
	userFilter bson.D,
	order []string,
	after string,
	limit int,
	fields []string,
) ([]bson.M, error) {

	pipe := []bson.M{}

	// Add sharding match
	if shardFilter != nil {
		pipe = append(pipe, bson.M{"$match": shardFilter})
	}

	// Add namespace match
	if namespaceFiler != nil {
		pipe = append(pipe, bson.M{"$match": namespaceFiler})
	}

	// Add forced match
	if forcedReadFilter != nil {
		pipe = append(pipe, bson.M{"$match": forcedReadFilter})
	}

	// Ordering
	if len(order) == 0 && after != "" {
		order = []string{"_id"}
	}

	if len(order) > 0 {

		var id bson.ObjectId
		doc := bson.M{}
		match := []bson.M{}
		sort := bson.D{}
		hasID := false

		// If we have an after, we get the previous object info
		if after != "" {

			if oid, ok := objectid.Parse(after); ok {
				id = oid
			} else {
				return nil, HandleQueryError(fmt.Errorf("after '%s' is not parsable objectId", after))
			}

			var err error
			if doc, err = retriever(id); err != nil {
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

			sort = append(sort, bson.DocElem{Name: f, Value: cmp})

			if after != "" {
				if f == "_id" {
					match = append(match,
						bson.M{"_id": bson.M{"$gt": id}},
					)
				} else {
					match = append(match,
						bson.M{
							"$or": []any{
								bson.M{f: bson.M{op: doc[f]}},
								bson.M{f: doc[f], "_id": bson.M{"$gt": id}},
							},
						},
					)
				}
			}
		}

		if !hasID {
			sort = append(sort, bson.DocElem{Name: "_id", Value: 1})
		}

		pipe = append(pipe, bson.M{"$sort": sort})

		if after != "" {
			pipe = append(pipe, bson.M{
				"$match": bson.M{"$and": match},
			})
		}
	}

	// User filtering
	if userFilter != nil {
		pipe = append(pipe, bson.M{"$match": userFilter})
	}

	// Limiting
	if limit > 0 {
		pipe = append(pipe, bson.M{"$limit": limit})
	}

	// Fields
	if sels := makeFieldsSelector(fields, attrSpec); sels != nil {
		pipe = append(pipe, bson.M{"$project": sels})
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

// HandleQueryError handles the provided upstream error returned by Mongo by returning a corresponding manipulate error type.
func HandleQueryError(err error) error {

	if _, ok := err.(net.Error); ok {
		return manipulate.ErrCannotCommunicate{Err: err}
	}

	if err == mgo.ErrNotFound {
		return manipulate.ErrObjectNotFound{Err: fmt.Errorf("cannot find the object for the given ID")}
	}

	if mgo.IsDup(err) {
		return manipulate.ErrConstraintViolation{Err: fmt.Errorf("duplicate key")}
	}

	if isConnectionError(err) {
		return manipulate.ErrCannotCommunicate{Err: err}
	}

	if ok, err := invalidQuery(err); ok {
		return err
	}

	// see https://github.com/mongodb/mongo/blob/master/src/mongo/base/error_codes.err
	switch getErrorCode(err) {
	case 6, 7, 71, 74, 91, 109, 189, 202, 216, 262, 10107, 13436, 13435, 11600, 11602:
		// HostUnreachable
		// HostNotFound,
		// ReplicaSetNotFound,
		// NodeNotFound,
		// ConfigurationInProgress,
		// ShutdownInProgress
		// PrimarySteppedDown,
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

func getErrorCode(err error) int {

	switch e := err.(type) {

	case *mgo.QueryError:
		return e.Code

	case *mgo.LastError:
		return e.Code

	case *mgo.BulkError:
		// we just get the first
		for _, c := range e.Cases() {
			return getErrorCode(c.Err)
		}
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

func queryError(err error) (*mgo.QueryError, bool) {

	if err == nil {
		return nil, false
	}

	switch e := err.(type) {
	case *mgo.QueryError:
		return e, true
	case *mgo.BulkError:
		for _, c := range e.Cases() {
			return queryError(c.Err)
		}
	}

	return nil, false
}

func isConnectionError(err error) bool {

	if err == nil {
		return false
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
		err == io.EOF ||
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

		f = strings.ToLower(strings.TrimPrefix(f, descendingOrderPrefix))
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

func convertReadConsistency(c manipulate.ReadConsistency) mgo.Mode {
	switch c {
	case manipulate.ReadConsistencyEventual:
		return mgo.Eventual
	case manipulate.ReadConsistencyMonotonic:
		return mgo.Monotonic
	case manipulate.ReadConsistencyNearest:
		return mgo.Nearest
	case manipulate.ReadConsistencyStrong:
		return mgo.Strong
	case manipulate.ReadConsistencyWeakest:
		return mgo.SecondaryPreferred
	default:
		return -1
	}
}

func convertWriteConsistency(c manipulate.WriteConsistency) *mgo.Safe {
	switch c {
	case manipulate.WriteConsistencyNone:
		return nil
	case manipulate.WriteConsistencyStrong:
		return &mgo.Safe{WMode: "majority"}
	case manipulate.WriteConsistencyStrongest:
		return &mgo.Safe{WMode: "majority", J: true}
	default:
		return &mgo.Safe{}
	}
}

type explainable interface {
	Explain(result interface{}) error
}

func explainIfNeeded[T explainable](
	query T,
	filter bson.D,
	identity elemental.Identity,
	operation elemental.Operation,
	explainMap map[elemental.Identity]map[elemental.Operation]struct{},
) func() error {

	if len(explainMap) == 0 {
		return nil
	}

	exp, ok := explainMap[identity]
	if !ok {
		return nil
	}

	if len(exp) == 0 {
		return func() error { return explain(query, operation, identity, filter) }
	}

	if _, ok = exp[operation]; ok {
		return func() error { return explain(query, operation, identity, filter) }
	}

	return nil
}

func explain[T explainable](query T, operation elemental.Operation, identity elemental.Identity, filter bson.D) error {

	r := bson.M{}
	if err := query.Explain(&r); err != nil {
		return fmt.Errorf("unable to explain: %s", err)
	}

	f := "<none>"
	if filter != nil {
		fdata, err := json.MarshalIndent(filter, "", "  ")
		if err != nil {
			return fmt.Errorf("unable to marshal filter: %s", err)
		}
		f = string(fdata)
	}

	rdata, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("unable to marshal explanation: %s", err)
	}

	fmt.Println("")
	fmt.Println("--------------------------------")
	fmt.Printf("Operation:  %s\n", operation)
	fmt.Printf("Identity:   %s\n", identity.Name)
	fmt.Printf("Filter:     %s\n", f)
	fmt.Println("Explanation:")
	fmt.Println(string(rdata))
	fmt.Println("--------------------------------")
	fmt.Println("")

	return nil
}

type maxable[T any] interface {
	SetMaxTime(d time.Duration) T
}

func setMaxTime[T maxable[T]](ctx context.Context, q T) (T, error) {

	d, ok := ctx.Deadline()
	if !ok {
		return q.SetMaxTime(defaultGlobalContextTimeout), nil
	}

	mx := time.Until(d)
	if err := ctx.Err(); err != nil {
		var zero T
		return zero, manipulate.ErrCannotBuildQuery{Err: err}
	}

	return q.SetMaxTime(mx), nil
}
