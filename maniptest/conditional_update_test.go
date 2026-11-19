package maniptest

import (
	"testing"

	"go.acuvity.ai/elemental"
	testmodel "go.acuvity.ai/elemental/test/model"
	"go.acuvity.ai/manipulate"
)

func TestConditionalUpdateDoesNotCallOrdinaryMock(t *testing.T) {
	m := NewTestManipulator()
	base := manipulate.NewContext(t.Context())
	ctx := base.Derive(manipulate.ContextOptionConditionalUpdate(manipulate.ConditionalUpdate{
		RevisionField: "revision", ExpectedRevision: 7,
	}))
	calls := 0
	m.MockUpdate(t, func(manipulate.Context, elemental.Identifiable) error {
		calls++
		return nil
	})

	patch := testmodel.NewSparseTask()
	if err := m.Update(ctx, patch); !manipulate.IsNotImplementedError(err) || calls != 0 {
		t.Fatalf("conditional update reached ordinary mock: %v, calls=%d", err, calls)
	}
	if err := m.Update(base, patch); err != nil || calls != 1 {
		t.Fatalf("ordinary update did not use the mock: %v, calls=%d", err, calls)
	}
}
