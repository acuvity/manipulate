package manipmongo

import (
	"testing"

	mongobson "go.mongodb.org/mongo-driver/v2/bson"
)

func TestCloneBSONValueRoundTrip(t *testing.T) {
	id := mongobson.NewObjectID()
	input := mongobson.D{
		{Key: "_id", Value: id},
		{Key: "name", Value: "demo"},
		{Key: "nested", Value: mongobson.D{{Key: "enabled", Value: true}}},
	}

	out, err := cloneBSONValue[mongobson.D](input)
	if err != nil {
		t.Fatalf("unexpected clone error: %v", err)
	}

	if len(out) != len(input) {
		t.Fatalf("expected %d elements, got %d", len(input), len(out))
	}
	if out[0].Key != "_id" {
		t.Fatalf("unexpected first key: %q", out[0].Key)
	}
	outID, ok := out[0].Value.(mongobson.ObjectID)
	if !ok {
		t.Fatalf("expected object id in first field, got %T", out[0].Value)
	}
	if outID != id {
		t.Fatalf("expected object id %v, got %v", id, outID)
	}
}

func TestCloneBSONValueNilInput(t *testing.T) {
	out, err := cloneBSONValue[mongobson.D](nil)
	if err != nil {
		t.Fatalf("unexpected nil clone error: %v", err)
	}
	if out != nil {
		t.Fatalf("expected nil output for nil input, got %#v", out)
	}
}

func TestCloneBSONValueIncompatibleTarget(t *testing.T) {
	if _, err := cloneBSONValue[int](mongobson.M{"a": 1}); err == nil {
		t.Fatalf("expected unmarshal error for incompatible target type")
	}
}
