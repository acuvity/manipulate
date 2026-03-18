package manipmongo

import (
	"testing"

	"go.acuvity.ai/elemental"
)

func TestShouldExplain(t *testing.T) {
	identity := elemental.MakeIdentity("item", "items")
	otherIdentity := elemental.MakeIdentity("other", "others")

	tests := []struct {
		name       string
		identity   elemental.Identity
		operation  elemental.Operation
		explainMap map[elemental.Identity]map[elemental.Operation]struct{}
		want       bool
	}{
		{
			name:      "empty explain map",
			identity:  identity,
			operation: elemental.OperationRetrieve,
			want:      false,
		},
		{
			name:      "identity not configured",
			identity:  identity,
			operation: elemental.OperationRetrieve,
			explainMap: map[elemental.Identity]map[elemental.Operation]struct{}{
				otherIdentity: {},
			},
			want: false,
		},
		{
			name:      "all operations enabled for identity",
			identity:  identity,
			operation: elemental.OperationRetrieve,
			explainMap: map[elemental.Identity]map[elemental.Operation]struct{}{
				identity: {},
			},
			want: true,
		},
		{
			name:      "specific operation enabled",
			identity:  identity,
			operation: elemental.OperationRetrieveMany,
			explainMap: map[elemental.Identity]map[elemental.Operation]struct{}{
				identity: {
					elemental.OperationRetrieveMany: {},
				},
			},
			want: true,
		},
		{
			name:      "specific operation not enabled",
			identity:  identity,
			operation: elemental.OperationInfo,
			explainMap: map[elemental.Identity]map[elemental.Operation]struct{}{
				identity: {
					elemental.OperationRetrieveMany: {},
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldExplain(tt.identity, tt.operation, tt.explainMap); got != tt.want {
				t.Fatalf("shouldExplain() = %v, want %v", got, tt.want)
			}
		})
	}
}
