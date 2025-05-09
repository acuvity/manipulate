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

package manipulate

import (
	"context"
	"fmt"
	"net/url"

	"go.acuvity.ai/elemental"
)

// ReadConsistency represents the desired consistency of the request.
// Not all driver may implement this.
type ReadConsistency string

// Various values for Consistency
const (
	ReadConsistencyDefault   ReadConsistency = "default"
	ReadConsistencyNearest   ReadConsistency = "nearest"
	ReadConsistencyEventual  ReadConsistency = "eventual"
	ReadConsistencyMonotonic ReadConsistency = "monotonic"
	ReadConsistencyStrong    ReadConsistency = "strong"
	ReadConsistencyWeakest   ReadConsistency = "weakest"
)

// WriteConsistency represents the desired consistency of the request.
// Not all driver may implement this.
type WriteConsistency string

// Various values for Consistency
const (
	WriteConsistencyDefault   WriteConsistency = "default"
	WriteConsistencyNone      WriteConsistency = "none"
	WriteConsistencyStrong    WriteConsistency = "strong"
	WriteConsistencyStrongest WriteConsistency = "strongest"
)

// A FinalizerFunc is the type of a function that can be used as a creation finalizer.
// This is only supported by manipulators that generate an ID to let a chance to the user.
// to now the intended ID before actually creating the object.
type FinalizerFunc func(o elemental.Identifiable) error

// A RetryFunc is a function that can be called during an
// auto retry.
// The current manipulate.Context is given, a Stringer interface containing,
// more info about the current request, the error that
// caused the retry and the try number.
// If this function returns an error, the retry procedure will
// be interupted and this error will be returns to the caller
// of the operation.
type RetryFunc func(RetryInfo) error

// A RetryInfo is the interface that can be passed to RetryFunc
// that will contain retry information. Content will depend on
// the manipulator implementation.
type RetryInfo interface {
	Err() error
	Context() Context
	Try() int
}

// A Context holds all information regarding a particular manipulate operation.
type Context interface {
	Count() int
	SetCount(count int)
	Filter() *elemental.Filter
	Finalizer() FinalizerFunc
	Version() int
	TransactionID() TransactionID
	Page() int
	PageSize() int
	After() string
	Limit() int
	Next() string
	SetNext(string)
	Override() bool
	Recursive() bool
	Namespace() string
	Propagated() bool
	Credentials() (string, string)
	Parameters() url.Values
	Parent() elemental.Identifiable
	ExternalTrackingID() string
	ExternalTrackingType() string
	Order() []string
	Context() context.Context
	Derive(...ContextOption) Context
	Fields() []string
	ReadConsistency() ReadConsistency
	WriteConsistency() WriteConsistency
	Messages() []string
	SetMessages([]string)
	ClientIP() string
	RetryFunc() RetryFunc
	RetryRatio() int64

	fmt.Stringer
}

type mcontext struct {
	ctx                  context.Context
	parent               elemental.Identifiable
	filter               *elemental.Filter
	opaque               map[string]any
	createFinalizer      FinalizerFunc
	retryFunc            RetryFunc
	parameters           url.Values
	password             string
	next                 string
	username             string
	namespace            string
	transactionID        TransactionID
	externalTrackingID   string
	idempotencyKey       string
	writeConsistency     WriteConsistency
	readConsistency      ReadConsistency
	after                string
	clientIP             string
	externalTrackingType string
	fields               []string
	order                []string
	messages             []string
	version              int
	limit                int
	pageSize             int
	page                 int
	retryRatio           int64
	countTotal           int
	overrideProtection   bool
	recursive            bool
	propagated           bool
}

// NewContext creates a context with the given ContextOption.
func NewContext(ctx context.Context, options ...ContextOption) Context {

	if ctx == nil {
		panic("nil context")
	}

	mctx := &mcontext{
		ctx:              ctx,
		writeConsistency: WriteConsistencyDefault,
		readConsistency:  ReadConsistencyDefault,
		retryRatio:       4,
		opaque:           map[string]any{},
	}

	for _, opt := range options {
		opt(mctx)
	}

	return mctx
}

// Derive creates a copy of the context but updates the values of the given options.
// Values that are parts of a response like Count or Messages or IdempotencyKey
// are reset for the derived context.
func (c *mcontext) Derive(options ...ContextOption) Context {

	var opaqueCopy map[string]any
	if len(c.opaque) > 0 {
		opaqueCopy = make(map[string]any, len(c.opaque))
		for k, v := range c.opaque {
			opaqueCopy[k] = v
		}
	}

	var paramsCopy url.Values
	if len(c.parameters) > 0 {
		paramsCopy = url.Values{}
		for k, v := range c.parameters {
			paramsCopy[k] = v
		}
	}

	copied := &mcontext{
		clientIP:             c.clientIP,
		createFinalizer:      c.createFinalizer,
		ctx:                  c.ctx,
		externalTrackingID:   c.externalTrackingID,
		externalTrackingType: c.externalTrackingType,
		fields:               append([]string{}, c.fields...),
		filter:               c.filter,
		namespace:            c.namespace,
		propagated:           c.propagated,
		order:                append([]string{}, c.order...),
		overrideProtection:   c.overrideProtection,
		page:                 c.page,
		pageSize:             c.pageSize,
		after:                c.after,
		limit:                c.limit,
		parameters:           paramsCopy,
		parent:               c.parent,
		password:             c.password,
		readConsistency:      c.readConsistency,
		recursive:            c.recursive,
		retryFunc:            c.retryFunc,
		retryRatio:           c.retryRatio,
		transactionID:        c.transactionID,
		username:             c.username,
		version:              c.version,
		writeConsistency:     c.writeConsistency,
		opaque:               opaqueCopy,
	}

	for _, opt := range options {
		opt(copied)
	}

	return copied
}

// Count returns the count
func (c *mcontext) Count() int { return c.countTotal }

// SetCount returns the total count.
func (c *mcontext) SetCount(count int) { c.countTotal = count }

// Filter returns the filter.
func (c *mcontext) Filter() *elemental.Filter { return c.filter }

// Finalizer returns the finalizer.
func (c *mcontext) Finalizer() FinalizerFunc { return c.createFinalizer }

// Version returns the version.
func (c *mcontext) Version() int { return c.version }

// TransactionID returns the transactionID.
func (c *mcontext) TransactionID() TransactionID { return c.transactionID }

// Page returns the page number.
func (c *mcontext) Page() int { return c.page }

// PageSize returns the pageSize.
func (c *mcontext) PageSize() int { return c.pageSize }

// After returns the after parameter used in lazy pagination.
func (c *mcontext) After() string { return c.after }

// Limit returns the limit parameter used in lazy pagination.
func (c *mcontext) Limit() int { return c.limit }

// After returns the after parameter used in lazy pagination.
func (c *mcontext) Next() string { return c.next }

// After returns the after parameter used in lazy pagination.
func (c *mcontext) SetNext(next string) { c.next = next }

// Override returns the override value.
func (c *mcontext) Override() bool { return c.overrideProtection }

// Recursive returns the recursive value.
func (c *mcontext) Recursive() bool { return c.recursive }

// Namespace returns the namespace value.
func (c *mcontext) Namespace() string { return c.namespace }

// Propagated returns the propagate value
func (c *mcontext) Propagated() bool { return c.propagated }

// Parameters returns the parameters.
func (c *mcontext) Parameters() url.Values { return c.parameters }

// Parent returns the parent.
func (c *mcontext) Parent() elemental.Identifiable { return c.parent }

// ExternalTrackingID returns the ExternalTrackingID.
func (c *mcontext) ExternalTrackingID() string { return c.externalTrackingID }

// ExternalTrackingType returns the ExternalTrackingType.
func (c *mcontext) ExternalTrackingType() string { return c.externalTrackingType }

// Order returns the Order.
func (c *mcontext) Order() []string { return c.order }

// Fields returns the fields.
func (c *mcontext) Fields() []string { return c.fields }

// WriteConsistency returns the desired write consistency.
func (c *mcontext) WriteConsistency() WriteConsistency { return c.writeConsistency }

// ReadConsistency returns the desired read consistency.
func (c *mcontext) ReadConsistency() ReadConsistency { return c.readConsistency }

// Messages returns the eventual list of messages regarding a manipulation.
func (c *mcontext) Messages() []string { return c.messages }

// SetMessages sets the message in the context.
// You should not need to use this.
func (c *mcontext) SetMessages(messages []string) { c.messages = messages }

// Context returns the internal context.Context.
func (c *mcontext) Context() context.Context { return c.ctx }

// IdempotencyKey returns the idempotency key.
func (c *mcontext) IdempotencyKey() string { return c.idempotencyKey }

// SetIdempotencyKey sets the IdempotencyKey. This is used internally
// by manipulator implementation supporting it.
func (c *mcontext) SetIdempotencyKey(k string) { c.idempotencyKey = k }

// DelegationToken returns any delegation token provided by options.
func (c *mcontext) Credentials() (string, string) { return c.username, c.password }

// Headers returns the optional headers.
func (c *mcontext) ClientIP() string { return c.clientIP }

// RetryRatio returns the context retry ratio.
func (c *mcontext) RetryRatio() int64 { return c.retryRatio }

// RetryFunc returns the retry function that is called when a retry occurs.
// If this function returns an error, retrying stops and the returned error
// returned by the manipulate operation.
func (c *mcontext) RetryFunc() RetryFunc { return c.retryFunc }

// Opaque returns the context opaque data.
func (c *mcontext) Opaque() map[string]any { return c.opaque }

// SetDelegationToken sets the delegation token for this context.
func (c *mcontext) SetCredentials(username, password string) {
	c.username = username
	c.password = password
}

// String returns the string representation of the Context.
func (c *mcontext) String() string {

	return fmt.Sprintf("<Context page:%d pagesize:%d filter:%v version:%d>", c.page, c.pageSize, c.filter, c.version)
}
