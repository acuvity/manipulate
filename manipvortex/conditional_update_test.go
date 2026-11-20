package manipvortex

import (
	"testing"

	"go.acuvity.ai/manipulate"
)

func TestConditionalUpdateUnsupported(t *testing.T) {
	var m *vortexManipulator
	ctx := manipulate.NewContext(t.Context(), manipulate.ContextOptionConditionalUpdate(manipulate.ConditionalUpdate{}))
	if err := m.Update(ctx, nil); !manipulate.IsNotImplementedError(err) {
		t.Fatalf("unsupported conditional update returned %v", err)
	}
}
