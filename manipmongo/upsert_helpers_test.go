package manipmongo

import (
	"reflect"
	"strings"
	"testing"

	bson "go.mongodb.org/mongo-driver/v2/bson"
)

func TestWorkerAConvertSetOnInsertToMongoMap(t *testing.T) {
	out, err := convertSetOnInsertToMongoMap(bson.M{"name": "demo"})
	if err != nil {
		t.Fatalf("unexpected conversion error: %v", err)
	}
	if out["name"] != "demo" {
		t.Fatalf("unexpected map content: %#v", out)
	}

	var nilMap bson.M
	out, err = convertSetOnInsertToMongoMap(nilMap)
	if err != nil {
		t.Fatalf("unexpected nil conversion error: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("expected empty map for nil input, got %#v", out)
	}

	if _, err := convertSetOnInsertToMongoMap("bad"); err == nil || !strings.Contains(err.Error(), "$setOnInsert operations must be bson.M") {
		t.Fatalf("expected invalid type error, got %v", err)
	}
}

func TestWorkerAMergeSetOnInsertOperationsMongo(t *testing.T) {
	dst := bson.M{"existing": "value"}
	if err := mergeSetOnInsertOperationsMongo(dst, bson.M{"new": 7}); err != nil {
		t.Fatalf("unexpected merge error: %v", err)
	}

	want := bson.M{"existing": "value", "new": 7}
	if !reflect.DeepEqual(dst, want) {
		t.Fatalf("unexpected merged map:\n got: %#v\nwant: %#v", dst, want)
	}

	if err := mergeSetOnInsertOperationsMongo(dst, "bad"); err == nil || !strings.Contains(err.Error(), "$setOnInsert operations must be bson.M") {
		t.Fatalf("expected invalid type merge error, got %v", err)
	}
}
