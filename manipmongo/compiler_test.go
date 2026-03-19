// Copyright 2019 Aporeto Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package manipmongo

import (
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"go.acuvity.ai/elemental"
	"go.acuvity.ai/manipulate/manipmongo/internal"
	bson "go.mongodb.org/mongo-driver/v2/bson"
)

func mustExtJSON(t *testing.T, value any) string {
	t.Helper()

	out, err := bson.MarshalExtJSON(value, false, false)
	if err != nil {
		t.Fatalf("failed to marshal BSON to extended JSON: %v", err)
	}

	return strings.ReplaceAll(string(out), "\n", "")
}

func TestCompilerOption(t *testing.T) {
	tests := map[string]struct {
		shouldPanic bool
		verify      func(t *testing.T)
	}{
		"CompilerOptionTranslateKeysFromSpec: basic": {
			verify: func(t *testing.T) {
				config := &compilerConfig{}
				CompilerOptionTranslateKeysFromSpec(&internal.MockAttributeSpecifiable{})(config)

				if !config.translateKeysFromSpec {
					t.Fatalf("expected translateKeysFromSpec to be true")
				}
				if config.attrSpec == nil {
					t.Fatalf("expected attrSpec to be configured")
				}
			},
		},
		"CompilerOptionTranslateKeysFromSpec: nil spec should panic": {
			shouldPanic: true,
			verify: func(t *testing.T) {
				config := &compilerConfig{}
				CompilerOptionTranslateKeysFromSpec(nil)(config)
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			defer func() {
				r := recover()
				if tc.shouldPanic && r == nil {
					t.Fatalf("expected panic")
				}
				if !tc.shouldPanic && r != nil {
					t.Fatalf("did not expect panic, got %v", r)
				}
			}()

			tc.verify(t)
		})
	}
}

func TestCompiler_WithCompilerOption(t *testing.T) {
	tests := map[string]struct {
		filter *elemental.Filter
		setup  func(t *testing.T, ctrl *gomock.Controller) []CompilerOption
		want   string
	}{
		"CompilerOptionTranslateKeysFromSpec should lookup filter keys from provided spec": {
			filter: elemental.NewFilterComposer().
				WithKey("field_a").Equals("test_value").
				Done(),
			setup: func(t *testing.T, ctrl *gomock.Controller) []CompilerOption {
				spec := internal.NewMockAttributeSpecifiable(ctrl)
				spec.EXPECT().
					SpecificationForAttribute("field_a").
					Return(elemental.AttributeSpecification{BSONFieldName: "a"})

				return []CompilerOption{CompilerOptionTranslateKeysFromSpec(spec)}
			},
			want: `{"$and":[{"a":{"$eq":"test_value"}}]}`,
		},
		"CompilerOptionTranslateKeysFromSpec should default to the key name if no entry found in the provided spec": {
			filter: elemental.NewFilterComposer().
				WithKey("field_a").Equals("test_value").
				WithKey("field_b").Equals("test_value").
				Done(),
			setup: func(t *testing.T, ctrl *gomock.Controller) []CompilerOption {
				spec := internal.NewMockAttributeSpecifiable(ctrl)
				spec.EXPECT().
					SpecificationForAttribute("field_a").
					Return(elemental.AttributeSpecification{BSONFieldName: "a"})
				spec.EXPECT().
					SpecificationForAttribute("field_b").
					Return(elemental.AttributeSpecification{})

				return []CompilerOption{CompilerOptionTranslateKeysFromSpec(spec)}
			},
			want: `{"$and":[{"a":{"$eq":"test_value"}},{"field_b":{"$eq":"test_value"}}]}`,
		},
		"CompilerOptionTranslateKeysFromSpec should be able to handle nested filters": {
			filter: elemental.NewFilterComposer().
				WithKey("field_a").Equals("test_value").
				And(
					elemental.NewFilterComposer().
						WithKey("field_b").Equals("test_value").
						WithKey("field_c").Equals("test_value").
						Done(),
					elemental.NewFilterComposer().
						WithKey("field_d").Equals("test_value").
						Or(
							elemental.NewFilterComposer().WithKey("field_e").Equals("test_value").Done(),
							elemental.NewFilterComposer().WithKey("field_f").Equals("test_value").Done(),
							elemental.NewFilterComposer().WithKey("field_g").NotIn("test_value_a", "test_value_b", "test_value_c").Done(),
						).
						Done(),
				).
				Done(),
			setup: func(t *testing.T, ctrl *gomock.Controller) []CompilerOption {
				spec := internal.NewMockAttributeSpecifiable(ctrl)
				spec.EXPECT().SpecificationForAttribute("field_a").Return(elemental.AttributeSpecification{BSONFieldName: "a"})
				spec.EXPECT().SpecificationForAttribute("field_b").Return(elemental.AttributeSpecification{BSONFieldName: "b"})
				spec.EXPECT().SpecificationForAttribute("field_c").Return(elemental.AttributeSpecification{BSONFieldName: "c"})
				spec.EXPECT().SpecificationForAttribute("field_d").Return(elemental.AttributeSpecification{BSONFieldName: "d"})
				spec.EXPECT().SpecificationForAttribute("field_e").Return(elemental.AttributeSpecification{BSONFieldName: "e"})
				spec.EXPECT().SpecificationForAttribute("field_f").Return(elemental.AttributeSpecification{BSONFieldName: "f"})
				spec.EXPECT().SpecificationForAttribute("field_g").Return(elemental.AttributeSpecification{BSONFieldName: "g"})

				return []CompilerOption{CompilerOptionTranslateKeysFromSpec(spec)}
			},
			want: `{"$and":[{"a":{"$eq":"test_value"}},{"$and":[{"$and":[{"b":{"$eq":"test_value"}},{"c":{"$eq":"test_value"}}]},{"$and":[{"d":{"$eq":"test_value"}},{"$or":[{"$and":[{"e":{"$eq":"test_value"}}]},{"$and":[{"f":{"$eq":"test_value"}}]},{"$and":[{"g":{"$nin":["test_value_a","test_value_b","test_value_c"]}}]}]}]}]}]}`,
		},
		"CompilerOptionTranslateKeysFromSpec should be able to handle filter with different casing": {
			filter: elemental.NewFilterComposer().
				WithKey("FiElD_A").Equals("test_value").
				Done(),
			setup: func(t *testing.T, ctrl *gomock.Controller) []CompilerOption {
				spec := internal.NewMockAttributeSpecifiable(ctrl)
				spec.EXPECT().
					SpecificationForAttribute("field_a").
					Return(elemental.AttributeSpecification{BSONFieldName: "a"})

				return []CompilerOption{CompilerOptionTranslateKeysFromSpec(spec)}
			},
			want: `{"$and":[{"a":{"$eq":"test_value"}}]}`,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			got := mustExtJSON(t, CompileFilter(tc.filter, tc.setup(t, ctrl)...))
			if got != tc.want {
				t.Fatalf("unexpected compiled filter\nwant: %s\n got: %s", tc.want, got)
			}
		})
	}
}

func TestUtils_compiler(t *testing.T) {
	tests := map[string]struct {
		filter     *elemental.Filter
		want       string
		wantPrefix string
	}{
		"empty filter": {
			filter: elemental.NewFilterComposer().Done(),
			want:   `{}`,
		},
		"simple filter with id": {
			filter: elemental.NewFilterComposer().WithKey("id").Equals("5d83e7eedb40280001887565").Done(),
			want:   `{"$and":[{"_id":{"$eq":{"$oid":"5d83e7eedb40280001887565"}}}]}`,
		},
		"boolean true": {
			filter: elemental.NewFilterComposer().WithKey("bool").Equals(true).Done(),
			want:   `{"$and":[{"bool":{"$eq":true}}]}`,
		},
		"boolean false": {
			filter: elemental.NewFilterComposer().WithKey("bool").Equals(false).Done(),
			want:   `{"$and":[{"$or":[{"bool":{"$eq":false}},{"bool":{"$exists":false}}]}]}`,
		},
		"dot notation": {
			filter: elemental.NewFilterComposer().WithKey("X.TOTO.Titu").Equals(1).Done(),
			want:   `{"$and":[{"x.TOTO.Titu":{"$eq":1}}]}`,
		},
		"multiple equals": {
			filter: elemental.NewFilterComposer().WithKey("x").Equals(1).WithKey("y").Equals(2).Done(),
			want:   `{"$and":[{"x":{"$eq":1}},{"y":{"$eq":2}}]}`,
		},
		"multiple not equals": {
			filter: elemental.NewFilterComposer().WithKey("x").NotEquals(1).WithKey("x").NotEquals(2).Done(),
			want:   `{"$and":[{"x":{"$ne":1}},{"x":{"$ne":2}}]}`,
		},
		"complex comparator mix": {
			filter: elemental.NewFilterComposer().
				WithKey("x").Equals(1).
				WithKey("z").Contains("a", "b").
				WithKey("a").GreaterOrEqualThan(1).
				WithKey("b").LesserOrEqualThan(1).
				WithKey("c").GreaterThan(1).
				WithKey("d").LesserThan(1).
				Done(),
			want: `{"$and":[{"x":{"$eq":1}},{"z":{"$in":["a","b"]}},{"a":{"$gte":1}},{"b":{"$lte":1}},{"c":{"$gt":1}},{"d":{"$lt":1}}]}`,
		},
		"match comparator": {
			filter: elemental.NewFilterComposer().WithKey("x").Matches("$abc^", ".*").Done(),
			want:   `{"$and":[{"$or":[{"x":{"$regex":"$abc^"}},{"x":{"$regex":".*"}}]}]}`,
		},
		"slash match comparator with i flag": {
			filter: elemental.NewFilterComposer().WithKey("x").Matches("/abc/i", ".*").Done(),
			want:   `{"$and":[{"$or":[{"x":{"$regex":"abc","$options":"i"}},{"x":{"$regex":".*"}}]}]}`,
		},
		"slash match comparator without i flag": {
			filter: elemental.NewFilterComposer().WithKey("x").Matches("/abc/", ".*").Done(),
			want:   `{"$and":[{"$or":[{"x":{"$regex":"abc"}},{"x":{"$regex":".*"}}]}]}`,
		},
		"incomplete slash match comparator": {
			filter: elemental.NewFilterComposer().WithKey("x").Matches("/abc", ".*/").Done(),
			want:   `{"$and":[{"$or":[{"x":{"$regex":"/abc"}},{"x":{"$regex":".*/"}}]}]}`,
		},
		"plain text ending with slash i is not normalized": {
			filter: elemental.NewFilterComposer().WithKey("x").Matches("abc/i", ".*/").Done(),
			want:   `{"$and":[{"$or":[{"x":{"$regex":"abc/i"}},{"x":{"$regex":".*/"}}]}]}`,
		},
		"exists": {
			filter: elemental.NewFilterComposer().WithKey("x").Exists().Done(),
			want:   `{"$and":[{"x":{"$exists":true}}]}`,
		},
		"duration": {
			filter:     elemental.NewFilterComposer().WithKey("x").Equals(3 * time.Second).Done(),
			wantPrefix: `{"$and":[{"x":{"$eq":{"$date":"`,
		},
		"not exists": {
			filter: elemental.NewFilterComposer().WithKey("x").NotExists().Done(),
			want:   `{"$and":[{"x":{"$exists":false}}]}`,
		},
		"valid uppercase ID": {
			filter: elemental.NewFilterComposer().WithKey("ID").Equals("5d85727b919e0c397a58e940").Done(),
			want:   `{"$and":[{"_id":{"$eq":{"$oid":"5d85727b919e0c397a58e940"}}}]}`,
		},
		"invalid uppercase ID": {
			filter: elemental.NewFilterComposer().WithKey("ID").Equals("not-object-id").Done(),
			want:   `{"$and":[{"_id":{"$eq":"not-object-id"}}]}`,
		},
		"valid lowercase id": {
			filter: elemental.NewFilterComposer().WithKey("id").Equals("5d85727b919e0c397a58e940").Done(),
			want:   `{"$and":[{"_id":{"$eq":{"$oid":"5d85727b919e0c397a58e940"}}}]}`,
		},
		"valid _id": {
			filter: elemental.NewFilterComposer().WithKey("_id").Equals("5d85727b919e0c397a58e940").Done(),
			want:   `{"$and":[{"_id":{"$eq":{"$oid":"5d85727b919e0c397a58e940"}}}]}`,
		},
		"in on valid ids": {
			filter: elemental.NewFilterComposer().WithKey("ID").In("5d85727b919e0c397a58e940", "5d85727b919e0c397a58e941").Done(),
			want:   `{"$and":[{"_id":{"$in":[{"$oid":"5d85727b919e0c397a58e940"},{"$oid":"5d85727b919e0c397a58e941"}]}}]}`,
		},
		"in on mixed ids": {
			filter: elemental.NewFilterComposer().WithKey("ID").In("not-object-id", "5d85727b919e0c397a58e941").Done(),
			want:   `{"$and":[{"_id":{"$in":["not-object-id",{"$oid":"5d85727b919e0c397a58e941"}]}}]}`,
		},
		"not in on valid ids": {
			filter: elemental.NewFilterComposer().WithKey("ID").NotIn("5d85727b919e0c397a58e940", "5d85727b919e0c397a58e941").Done(),
			want:   `{"$and":[{"_id":{"$nin":[{"$oid":"5d85727b919e0c397a58e940"},{"$oid":"5d85727b919e0c397a58e941"}]}}]}`,
		},
		"composed filters": {
			filter: elemental.NewFilterComposer().
				WithKey("namespace").Equals("coucou").
				And(
					elemental.NewFilterComposer().
						WithKey("name").Equals("toto").
						WithKey("surname").Equals("titi").
						Done(),
					elemental.NewFilterComposer().
						WithKey("color").Equals("blue").
						Or(
							elemental.NewFilterComposer().WithKey("size").Equals("big").Done(),
							elemental.NewFilterComposer().WithKey("size").Equals("medium").Done(),
							elemental.NewFilterComposer().WithKey("list").NotIn("a", "b", "c").Done(),
						).
						Done(),
				).
				Done(),
			want: `{"$and":[{"namespace":{"$eq":"coucou"}},{"$and":[{"$and":[{"name":{"$eq":"toto"}},{"surname":{"$eq":"titi"}}]},{"$and":[{"color":{"$eq":"blue"}},{"$or":[{"$and":[{"size":{"$eq":"big"}}]},{"$and":[{"size":{"$eq":"medium"}}]},{"$and":[{"list":{"$nin":["a","b","c"]}}]}]}]}]}]}`,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := mustExtJSON(t, CompileFilter(tc.filter))
			if tc.want != "" && got != tc.want {
				t.Fatalf("unexpected compiled filter\nwant: %s\n got: %s", tc.want, got)
			}
			if tc.wantPrefix != "" && !strings.HasPrefix(got, tc.wantPrefix) {
				t.Fatalf("unexpected compiled filter prefix\nwant prefix: %s\n got: %s", tc.wantPrefix, got)
			}
		})
	}
}
