package objectid

import (
	"reflect"
	"testing"

	bson "go.mongodb.org/mongo-driver/v2/bson"
)

func TestParse(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name  string
		args  args
		want  bson.ObjectID
		want1 bool
	}{
		{
			"valid",
			args{
				"5d66b8f7919e0c446f0b4597",
			},
			mustObjectID("5d66b8f7919e0c446f0b4597"),
			true,
		},
		{
			"not exa",
			args{
				"ZZZ6b8f7919e0c446f0b4597",
			},
			bson.NilObjectID,
			false,
		},
		{
			"too short",
			args{
				"5d66b8f7919e0c446f0b459",
			},
			bson.NilObjectID,
			false,
		},
		{
			"empty",
			args{
				"",
			},
			bson.NilObjectID,
			false,
		},
		{
			"weird stuff",
			args{
				"hello world how are you",
			},
			bson.NilObjectID,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := Parse(tt.args.s)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parse() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("Parse() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func mustObjectID(s string) bson.ObjectID {
	oid, err := bson.ObjectIDFromHex(s)
	if err != nil {
		panic(err)
	}
	return oid
}
