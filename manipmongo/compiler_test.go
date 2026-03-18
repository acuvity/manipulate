package manipmongo

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"go.acuvity.ai/elemental"
	bson "go.mongodb.org/mongo-driver/v2/bson"
)

type compilerTestAttributeSpec struct {
	fields map[string]string
}

func (s *compilerTestAttributeSpec) SpecificationForAttribute(name string) elemental.AttributeSpecification {
	if s == nil {
		return elemental.AttributeSpecification{}
	}
	if field, ok := s.fields[name]; ok {
		return elemental.AttributeSpecification{BSONFieldName: field}
	}
	return elemental.AttributeSpecification{}
}

func (s *compilerTestAttributeSpec) AttributeSpecifications() map[string]elemental.AttributeSpecification {
	out := make(map[string]elemental.AttributeSpecification, len(s.fields))
	for k, v := range s.fields {
		out[k] = elemental.AttributeSpecification{BSONFieldName: v}
	}
	return out
}

func (s *compilerTestAttributeSpec) ValueForAttribute(string) any { return nil }

func TestWorkerACompilerOptionTranslateKeysFromSpecPanicsOnNil(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic when nil spec is passed")
		}
	}()

	_ = CompilerOptionTranslateKeysFromSpec(nil)
}

func TestWorkerACompileFilterTranslatesKeysFromSpec(t *testing.T) {
	spec := &compilerTestAttributeSpec{fields: map[string]string{"tenantid": "tid"}}
	filter := elemental.NewFilterComposer().WithKey("tenantid").Equals("acuvity").Done()

	got := CompileFilter(filter, CompilerOptionTranslateKeysFromSpec(spec))
	want := bson.D{{
		Key: "$and",
		Value: []bson.D{{
			{Key: "tid", Value: bson.D{{Key: "$eq", Value: "acuvity"}}},
		}},
	}}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected compiled filter:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestWorkerACompileFilterConvertsIDHexToObjectID(t *testing.T) {
	idHex := "507f1f77bcf86cd799439011"
	filter := elemental.NewFilterComposer().WithKey("ID").Equals(idHex).Done()

	got := CompileFilter(filter)
	andItems, ok := got[0].Value.([]bson.D)
	if !ok || len(andItems) != 1 || len(andItems[0]) != 1 {
		t.Fatalf("unexpected $and shape: %#v", got)
	}

	eqDoc, ok := andItems[0][0].Value.(bson.D)
	if !ok || len(eqDoc) != 1 {
		t.Fatalf("unexpected equality doc: %#v", andItems[0][0].Value)
	}

	objectID, ok := eqDoc[0].Value.(bson.ObjectID)
	if !ok {
		t.Fatalf("expected bson.ObjectID, got %T", eqDoc[0].Value)
	}
	if objectID.Hex() != idHex {
		t.Fatalf("unexpected object id: got %s want %s", objectID.Hex(), idHex)
	}
}

func TestWorkerACompileFilterBooleanFalseIncludesExistsBranch(t *testing.T) {
	filter := elemental.NewFilterComposer().WithKey("enabled").Equals(false).Done()
	got := CompileFilter(filter)

	encoded, err := bson.MarshalExtJSON(got, false, false)
	if err != nil {
		t.Fatalf("unable to marshal compiled filter: %v", err)
	}

	asText := string(encoded)
	if !strings.Contains(asText, "\"$exists\":false") {
		t.Fatalf("expected compiled filter to include an $exists:false branch, got %s", asText)
	}
}

func TestWorkerACompileFilterWithNoOperatorsReturnsEmptyDocument(t *testing.T) {
	got := CompileFilter(elemental.NewFilter())
	if len(got) != 0 {
		t.Fatalf("expected empty document for empty filter, got %#v", got)
	}
}

func TestWorkerACompileFilterComparatorAndSubFilterBranches(t *testing.T) {
	filter := elemental.NewFilterComposer().
		WithKey("enabled").Equals(true).
		WithKey("name").NotEquals("blocked").
		WithKey("tags").In("a", "b").
		WithKey("contains").Contains("x", "y").
		WithKey("excluded").NotIn("c").
		WithKey("notcontains").NotContains("d").
		WithKey("gte").GreaterOrEqualThan(5).
		WithKey("gt").GreaterThan(6).
		WithKey("lte").LesserOrEqualThan(7).
		WithKey("lt").LesserThan(8).
		WithKey("exists").Exists().
		WithKey("missing").NotExists().
		WithKey("pattern").Matches("/abc/i", "/def/", "raw").
		And(
			elemental.NewFilterComposer().WithKey("left").Equals("L").Done(),
			elemental.NewFilterComposer().WithKey("right").Equals("R").Done(),
		).
		Or(
			elemental.NewFilterComposer().WithKey("alt").Equals("A").Done(),
			elemental.NewFilterComposer().WithKey("alt").Equals("B").Done(),
		).
		Done()

	got := CompileFilter(filter)
	encoded, err := bson.MarshalExtJSON(got, false, false)
	if err != nil {
		t.Fatalf("unable to marshal compiled filter: %v", err)
	}

	asText := string(encoded)
	expectedFragments := []string{
		"\"$eq\":true",
		"\"$ne\"",
		"\"$in\"",
		"\"$nin\"",
		"\"$gte\"",
		"\"$gt\"",
		"\"$lte\"",
		"\"$lt\"",
		"\"$exists\":true",
		"\"$exists\":false",
		"\"$regex\":\"abc\"",
		"\"$options\":\"i\"",
		"\"$regex\":\"def\"",
		"\"$regex\":\"raw\"",
	}

	for _, fragment := range expectedFragments {
		if !strings.Contains(asText, fragment) {
			t.Fatalf("expected compiled filter to include %s, got %s", fragment, asText)
		}
	}

	if strings.Contains(asText, "/def/") {
		t.Fatalf("expected slash-delimited regex literal to be normalized, got %s", asText)
	}

	if strings.Count(asText, "\"$and\"") < 2 {
		t.Fatalf("expected nested $and branch from AndFilterOperator, got %s", asText)
	}
	if strings.Count(asText, "\"$or\"") < 2 {
		t.Fatalf("expected nested $or branch from OrFilterOperator plus match comparator, got %s", asText)
	}
}

func TestWorkerAMassageKeyAndValueHelpers(t *testing.T) {
	if got := massageKey("Parent.Child"); got != "parent.Child" {
		t.Fatalf("unexpected nested key normalization: got %q want %q", got, "parent.Child")
	}
	if got := massageKey("ID"); got != "_id" {
		t.Fatalf("unexpected id key normalization: got %q want %q", got, "_id")
	}

	before := time.Now()
	afterDuration := massageValue("deadline", time.Second)
	asTime, ok := afterDuration.(time.Time)
	if !ok {
		t.Fatalf("expected time.Time for duration massage, got %T", afterDuration)
	}
	if asTime.Before(before) || asTime.After(time.Now().Add(2*time.Second)) {
		t.Fatalf("unexpected duration-based time value: %v", asTime)
	}

	validID := "507f1f77bcf86cd799439011"
	massagedID := massageValue("_id", validID)
	if _, ok := massagedID.(bson.ObjectID); !ok {
		t.Fatalf("expected bson.ObjectID conversion for valid _id string, got %T", massagedID)
	}

	invalidID := "not-a-valid-object-id"
	if got := massageValue("_id", invalidID); got != invalidID {
		t.Fatalf("expected invalid _id string to remain unchanged, got %#v", got)
	}
}

func TestWorkerAMassageValuesLoop(t *testing.T) {
	validID := "507f1f77bcf86cd799439011"
	values := massageValues("ID", []any{validID, "invalid"})
	if len(values) != 2 {
		t.Fatalf("unexpected values length: %#v", values)
	}
	if _, ok := values[0].(bson.ObjectID); !ok {
		t.Fatalf("expected first value to be converted to object id, got %T", values[0])
	}
	if values[1] != "invalid" {
		t.Fatalf("expected invalid value to remain unchanged, got %#v", values[1])
	}
}
