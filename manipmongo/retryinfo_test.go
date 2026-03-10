package manipmongo

import (
	"context"
	"errors"
	"testing"

	"go.acuvity.ai/manipulate"
)

func TestWorkerARetryInfoAccessors(t *testing.T) {
	retryErr := errors.New("retryable failure")
	mctx := manipulate.NewContext(
		context.Background(),
		manipulate.ContextOptionNamespace("tenant-a"),
	)

	info := RetryInfo{
		try:  4,
		err:  retryErr,
		mctx: mctx,
	}

	if got := info.Try(); got != 4 {
		t.Fatalf("unexpected try count: got %d want 4", got)
	}
	if got := info.Err(); !errors.Is(got, retryErr) {
		t.Fatalf("unexpected retry error: got %v want %v", got, retryErr)
	}
	if got := info.Context(); got == nil || got.Namespace() != "tenant-a" {
		t.Fatalf("unexpected retry context: %#v", got)
	}
}
