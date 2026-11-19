package manipulate

import (
	"context"
	"fmt"
	"testing"
	"time"
)

type embeddedConditionalContext = Context

type wrappedConditionalContext struct{ embeddedConditionalContext }

func TestConditionalUpdateOptionIsIsolated(t *testing.T) {
	deadline := time.Now().Add(time.Minute)
	native, cancel := context.WithDeadline(t.Context(), deadline)
	defer cancel()
	base := NewContext(native, ContextOptionNamespace("/owned"))
	check := ConditionalUpdate{RevisionField: "revision", ExpectedRevision: 7}
	ctx := wrappedConditionalContext{base.Derive(ContextOptionConditionalUpdate(check)).Derive(ContextOptionNamespace("/child"))}
	if got, present := ConditionalUpdateFromContext(ctx); !present || got != check {
		t.Fatalf("derived operation lost its revision check: %+v", got)
	}
	if _, present := ConditionalUpdateFromContext(base); present {
		t.Fatal("revision check leaked into the parent operation")
	}
	fresh := NewContext(ctx.Context(), ContextOptionNamespace("/fresh"))
	if _, present := ConditionalUpdateFromContext(fresh); present {
		t.Fatal("fresh operation inherited the revision check")
	}
	if got, ok := fresh.Context().Deadline(); !ok || !got.Equal(deadline) || fresh.Namespace() != "/fresh" {
		t.Fatalf("fresh operation changed deadline or scope: %v, %q", got, fresh.Namespace())
	}
	next := ConditionalUpdate{RevisionField: "revision", ExpectedRevision: 8}
	explicit := NewContext(ctx.Context(), ContextOptionConditionalUpdate(next))
	if got, present := ConditionalUpdateFromContext(explicit); !present || got != next {
		t.Fatalf("fresh operation lost its explicit revision check: %+v", got)
	}
	empty := base.Derive(ContextOptionConditionalUpdate(ConditionalUpdate{}))
	if _, present := ConditionalUpdateFromContext(empty); !present {
		t.Fatal("invalid revision check was treated as absent")
	}
	if _, present := ConditionalUpdateFromContext(nil); present {
		t.Fatal("nil context was marked conditional")
	}
}

func TestConditionalPreconditionError(t *testing.T) {
	err := fmt.Errorf("write failed: %w", ErrPreconditionFailed{})
	if !IsPreconditionFailedError(err) || IsObjectNotFoundError(err) || IsPreconditionFailedError(nil) {
		t.Fatal("conditional miss must be detectable independently of not-found")
	}
}
