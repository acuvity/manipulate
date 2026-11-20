package manipulate

import "context"

// ConditionalUpdate compares one stored integer revision before applying a sparse patch.
type ConditionalUpdate struct {
	RevisionField      string // Model attribute name, not the BSON key.
	ExpectedRevision   int64  // Nonnegative current revision. Update advances it by exactly one.
	AllowMissingAtZero bool   // At expected zero only, also match an absent revision on an existing record.
}

type conditionalUpdateKey struct{}

// ContextOptionConditionalUpdate makes Update compare the revision, set the
// supplied sparse fields, and increment the revision in one write. The patch
// must omit the revision field. Writers must advance the same field. Reusing a
// revision after a delete or restore can make an old check match.
// String identifiers and namespace paths match case-sensitively. Caller and
// forced filters still use the collection's collation.
//
// Use only on a known direct Mongo manipulator. Other built-in adapters return
// [ErrNotImplemented]. External adapters or wrappers may ignore the option.
// A conditional write requires acknowledgement and leaves the patch unchanged.
// If a mutable copy loses model state, Update returns [ErrCannotMarshal] before
// writing. Update reads back no document. A miss returns [ErrPreconditionFailed]
// without inserting a record. The Mongo driver honors the client's retryWrites
// setting, but Manipulate does not start a fresh attempt. An error can leave the
// outcome uncertain. Explicit Mongo sessions and transaction contexts are not supported.
func ContextOptionConditionalUpdate(check ConditionalUpdate) ContextOption {
	return func(c Context) {
		ctx := c.(*mcontext)
		ctx.ctx = context.WithValue(ctx.ctx, conditionalUpdateKey{}, check)
	}
}

// ConditionalUpdateFromContext reports whether this operation requested a
// revision check. An adapter that cannot enforce it must reject the update.
func ConditionalUpdateFromContext(ctx Context) (ConditionalUpdate, bool) {
	if ctx == nil {
		return ConditionalUpdate{}, false
	}
	native := ctx.Context()
	if native == nil {
		return ConditionalUpdate{}, false
	}
	check, ok := native.Value(conditionalUpdateKey{}).(ConditionalUpdate)
	return check, ok
}
