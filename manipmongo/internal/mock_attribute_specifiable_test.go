package internal

import (
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"go.acuvity.ai/elemental"
)

func TestMockAttributeSpecifiable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := NewMockAttributeSpecifiable(ctrl)
	if mock.EXPECT() == nil {
		t.Fatalf("expected recorder to be initialized")
	}

	expectedSpecs := map[string]elemental.AttributeSpecification{
		"name": {},
	}
	mock.EXPECT().AttributeSpecifications().Return(expectedSpecs)
	if got := mock.AttributeSpecifications(); !reflect.DeepEqual(got, expectedSpecs) {
		t.Fatalf("unexpected attribute specifications: got=%#v want=%#v", got, expectedSpecs)
	}

	expectedSpec := elemental.AttributeSpecification{}
	mock.EXPECT().SpecificationForAttribute("name").Return(expectedSpec)
	if got := mock.SpecificationForAttribute("name"); !reflect.DeepEqual(got, expectedSpec) {
		t.Fatalf("unexpected specification for attribute: got=%#v want=%#v", got, expectedSpec)
	}

	mock.EXPECT().ValueForAttribute("name").Return("value")
	if got := mock.ValueForAttribute("name"); got != "value" {
		t.Fatalf("unexpected value for attribute: got=%#v want=%#v", got, "value")
	}
}
