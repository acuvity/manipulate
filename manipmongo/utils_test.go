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
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"go.acuvity.ai/elemental"
	testmodel "go.acuvity.ai/elemental/test/model"
	"go.acuvity.ai/manipulate"
	"go.acuvity.ai/manipulate/internal/objectid"
	"go.acuvity.ai/manipulate/manipmongo/internal"
)

func Test_HandleQueryError(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name      string
		args      args
		errString string
	}{
		{
			"error 2",
			args{
				&mgo.QueryError{
					Code:    2,
					Message: errInvalidQueryBadRegex,
				},
			},
			"Query invalid: $regex has to be a string",
		},
		{
			"error 51091",
			args{
				&mgo.QueryError{
					Code:    51091,
					Message: errInvalidQueryInvalidRegex,
				},
			},
			"Query invalid: regular expression is invalid",
		},
		{
			"net error",
			args{
				&net.OpError{
					Op:  "coucou",
					Err: fmt.Errorf("network sucks"),
				},
			},
			"Cannot communicate: coucou: network sucks",
		},
		{
			"err not found",
			args{
				mgo.ErrNotFound,
			},
			"Object not found: cannot find the object for the given ID",
		},
		{
			"err dup",
			args{
				&mgo.LastError{Code: 11000},
			},
			"Constraint violation: duplicate key",
		},
		{
			"isConnectionError says yes",
			args{
				fmt.Errorf("lost connection to server"),
			},
			"Cannot communicate: lost connection to server",
		},
		{
			"isConnectionError says no",
			args{
				fmt.Errorf("no"),
			},
			"Unable to execute query: no",
		},

		{
			"err 6",
			args{
				&mgo.LastError{Code: 6, Err: "boom"},
			},
			"Cannot communicate: boom",
		},
		{
			"err 7",
			args{
				&mgo.LastError{Code: 7, Err: "boom"},
			},
			"Cannot communicate: boom",
		},
		{
			"err 71",
			args{
				&mgo.LastError{Code: 71, Err: "boom"},
			},
			"Cannot communicate: boom",
		},
		{
			"err 74",
			args{
				&mgo.LastError{Code: 74, Err: "boom"},
			},
			"Cannot communicate: boom",
		},
		{
			"err 91",
			args{
				&mgo.LastError{Code: 91, Err: "boom"},
			},
			"Cannot communicate: boom",
		},
		{
			"err 109",
			args{
				&mgo.LastError{Code: 109, Err: "boom"},
			},
			"Cannot communicate: boom",
		},
		{
			"err 189",
			args{
				&mgo.LastError{Code: 189, Err: "boom"},
			},
			"Cannot communicate: boom",
		},
		{
			"err 202",
			args{
				&mgo.LastError{Code: 202, Err: "boom"},
			},
			"Cannot communicate: boom",
		},
		{
			"err 216",
			args{
				&mgo.LastError{Code: 216, Err: "boom"},
			},
			"Cannot communicate: boom",
		},
		{
			"err 10107",
			args{
				&mgo.LastError{Code: 10107, Err: "boom"},
			},
			"Cannot communicate: boom",
		},
		{
			"err 13436",
			args{
				&mgo.LastError{Code: 13436, Err: "boom"},
			},
			"Cannot communicate: boom",
		},
		{
			"err 13435",
			args{
				&mgo.LastError{Code: 13435, Err: "boom"},
			},
			"Cannot communicate: boom",
		},
		{
			"err 11600",
			args{
				&mgo.LastError{Code: 11600, Err: "boom"},
			},
			"Cannot communicate: boom",
		},
		{
			"err 11602",
			args{
				&mgo.LastError{Code: 11602, Err: "boom"},
			},
			"Cannot communicate: boom",
		},
		{
			"err 424242",
			args{
				&mgo.LastError{Code: 424242, Err: "boom"},
			},
			"Unable to execute query: boom",
		},

		{
			"err 11602 QueryError ",
			args{
				&mgo.QueryError{Code: 424242, Message: "boom"},
			},
			"Unable to execute query: boom",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := HandleQueryError(tt.args.err)
			if tt.errString != err.Error() {
				t.Errorf("HandleQueryError() error = %v, wantErr %v", err, tt.errString)
			}
		})
	}
}

func Test_makeNamespaceFilter(t *testing.T) {

	Convey("calling with no ns in mctx should work", t, func() {
		mctx := manipulate.NewContext(context.Background())
		f := makeNamespaceFilter(mctx)
		So(f, ShouldBeNil)
	})

	Convey("calling with a ns in mctx should work", t, func() {
		mctx := manipulate.NewContext(context.Background(),
			manipulate.ContextOptionNamespace("/hello/world"),
		)
		f := makeNamespaceFilter(mctx)
		So(f, ShouldNotBeNil)
		So(f, ShouldResemble, bson.D{
			bson.DocElem{
				Name: "$and",
				Value: []bson.D{
					{
						bson.DocElem{
							Name: "namespace",
							Value: bson.D{
								bson.DocElem{
									Name:  "$eq",
									Value: "/hello/world",
								},
							},
						},
					},
				},
			},
		})
	})

	Convey("calling with a ns and recursive in mctx should work", t, func() {
		mctx := manipulate.NewContext(context.Background(),
			manipulate.ContextOptionNamespace("/hello/world"),
			manipulate.ContextOptionRecursive(true),
		)
		f := makeNamespaceFilter(mctx)
		So(f, ShouldNotBeNil)
		So(f, ShouldResemble, bson.D{
			bson.DocElem{
				Name: "$and",
				Value: []bson.D{
					{
						bson.DocElem{
							Name: "$or",
							Value: []bson.D{
								{
									bson.DocElem{
										Name: "$and",
										Value: []bson.D{
											{
												bson.DocElem{
													Name: "namespace",
													Value: bson.D{
														bson.DocElem{
															Name:  "$eq",
															Value: "/hello/world",
														},
													},
												},
											},
										},
									},
								},
								{
									bson.DocElem{
										Name: "$and",
										Value: []bson.D{
											{
												bson.DocElem{
													Name: "$or",
													Value: []bson.D{
														{
															bson.DocElem{
																Name: "namespace",
																Value: bson.D{
																	bson.DocElem{
																		Name:  "$regex",
																		Value: "^/hello/world/",
																	},
																},
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		})
	})

	Convey("calling with a ns and recursive and propagated in mctx should work", t, func() {
		mctx := manipulate.NewContext(context.Background(),
			manipulate.ContextOptionNamespace("/hello/world"),
			manipulate.ContextOptionRecursive(true),
			manipulate.ContextOptionPropagated(true),
		)
		f := makeNamespaceFilter(mctx)
		So(f, ShouldNotBeNil)
		So(f, ShouldResemble,
			bson.D{
				bson.DocElem{
					Name: "$and",
					Value: []bson.D{
						{
							bson.DocElem{
								Name: "$or",
								Value: []bson.D{
									{
										bson.DocElem{
											Name: "$and",
											Value: []bson.D{
												{
													bson.DocElem{
														Name: "$or",
														Value: []bson.D{
															{
																bson.DocElem{
																	Name: "$and",
																	Value: []bson.D{
																		{
																			bson.DocElem{
																				Name: "namespace",
																				Value: bson.D{
																					bson.DocElem{
																						Name:  "$eq",
																						Value: "/hello/world",
																					},
																				},
																			},
																		},
																	},
																},
															},
															{
																bson.DocElem{
																	Name: "$and",
																	Value: []bson.D{
																		{
																			bson.DocElem{
																				Name: "$or",
																				Value: []bson.D{
																					{
																						bson.DocElem{
																							Name: "namespace",
																							Value: bson.D{
																								bson.DocElem{
																									Name:  "$regex",
																									Value: "^/hello/world/",
																								},
																							},
																						},
																					},
																				},
																			},
																		},
																	},
																},
															},
														},
													},
												},
											},
										},
									},
									{
										bson.DocElem{
											Name: "$and",
											Value: []bson.D{
												{
													bson.DocElem{
														Name: "$or",
														Value: []bson.D{
															{
																bson.DocElem{
																	Name: "$and",
																	Value: []bson.D{
																		{
																			bson.DocElem{
																				Name:  "namespace",
																				Value: bson.D{bson.DocElem{Name: "$eq", Value: "/hello"}},
																			},
																		},
																		{
																			bson.DocElem{
																				Name:  "propagate",
																				Value: bson.M{"$eq": true},
																			},
																		},
																	},
																},
															},
															{
																bson.DocElem{
																	Name: "$and",
																	Value: []bson.D{
																		{
																			bson.DocElem{
																				Name:  "namespace",
																				Value: bson.D{bson.DocElem{Name: "$eq", Value: "/"}},
																			},
																		},
																		{
																			bson.DocElem{
																				Name:  "propagate",
																				Value: bson.M{"$eq": true},
																			},
																		},
																	},
																},
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		)
	})
}

func Test_makeUserFilter(t *testing.T) {

	Convey("making user filter with no filter should work", t, func() {

		attrSpec := testmodel.NewList()
		mctx := manipulate.NewContext(context.Background())
		f := makeUserFilter(mctx, attrSpec)
		So(f, ShouldBeNil)
	})

	Convey("making user filter with filter should work", t, func() {

		attrSpec := testmodel.NewList()
		mctx := manipulate.NewContext(context.Background(),
			manipulate.ContextOptionFilter(
				elemental.NewFilterComposer().WithKey("name").Equals("the-name").Done(),
			),
		)
		f := makeUserFilter(mctx, attrSpec)
		So(f, ShouldNotBeNil)
		So(f, ShouldResemble,
			bson.D{
				bson.DocElem{
					Name: "$and",
					Value: []bson.D{
						{
							bson.DocElem{
								Name: "name",
								Value: bson.D{
									bson.DocElem{
										Name:  "$eq",
										Value: "the-name",
									},
								},
							},
						},
					},
				},
			},
		)
	})
}

func Test_makePipeline(t *testing.T) {

	Convey("calling make pipeline with everything empty should work", t, func() {

		attrSpec := testmodel.NewList()
		r := func(id bson.ObjectId) (bson.M, error) { return nil, nil }

		pipe, err := makePipeline(attrSpec, r, nil, nil, nil, nil, nil, "", 0, nil)
		So(err, ShouldBeNil)
		So(pipe, ShouldResemble, []bson.M{})
	})

	Convey("calling make pipeline with everything ok should work", t, func() {

		attrSpec := testmodel.NewList()
		r := func(id bson.ObjectId) (bson.M, error) {
			return bson.M{"name": "bob", "age": 245}, nil
		}

		id, _ := objectid.Parse("673f8580686b0ea7a1241fee")

		pipe, err := makePipeline(
			attrSpec,
			r,
			bson.D{bson.DocElem{Name: "shard", Value: "good"}},
			bson.D{bson.DocElem{Name: "namespace", Value: "good"}},
			bson.D{bson.DocElem{Name: "forced", Value: "good"}},
			bson.D{bson.DocElem{Name: "user", Value: "good"}},
			[]string{"name", "-age"},
			"673f8580686b0ea7a1241fee",
			2,
			[]string{"a", "b"},
		)
		So(err, ShouldBeNil)
		So(pipe, ShouldResemble,
			[]bson.M{
				{"$match": bson.D{
					bson.DocElem{Name: "shard", Value: "good"},
				}},
				{"$match": bson.D{
					bson.DocElem{Name: "namespace", Value: "good"},
				}},
				{"$match": bson.D{
					bson.DocElem{Name: "forced", Value: "good"},
				}},
				{"$sort": bson.D{
					bson.DocElem{Name: "name", Value: 1},
					bson.DocElem{Name: "age", Value: -1},
					bson.DocElem{Name: "_id", Value: 1},
				}},
				{"$match": bson.M{
					"$and": []bson.M{
						{
							"$or": []interface{}{
								bson.M{"name": bson.M{"$gt": "bob"}},
								bson.M{"_id": bson.M{"$gt": id}, "name": "bob"},
							},
						},
						{
							"$or": []interface{}{
								bson.M{"age": bson.M{"$lt": 245}},
								bson.M{"_id": bson.M{"$gt": id}, "age": 245},
							},
						},
					},
				}},
				{"$match": bson.D{
					bson.DocElem{Name: "user", Value: "good"},
				}},
				{"$limit": 2},
				{"$project": bson.M{"a": 1, "b": 1}},
			},
		)
	})

	Convey("calling make pipeline with after and no order ok should work", t, func() {

		attrSpec := testmodel.NewList()
		r := func(id bson.ObjectId) (bson.M, error) {
			return bson.M{"name": "bob", "age": 245}, nil
		}

		id, _ := objectid.Parse("673f8580686b0ea7a1241fee")

		pipe, err := makePipeline(
			attrSpec,
			r,
			nil,
			nil,
			nil,
			nil,
			nil,
			"673f8580686b0ea7a1241fee",
			2,
			nil,
		)
		So(err, ShouldBeNil)
		So(pipe, ShouldResemble,
			[]bson.M{
				{"$sort": bson.D{
					bson.DocElem{Name: "_id", Value: 1},
				}},
				{"$match": bson.M{
					"$and": []bson.M{
						{"_id": bson.M{"$gt": id}},
					},
				}},
				{"$limit": 2},
			},
		)
	})

	Convey("calling make pipeline with bad after format work", t, func() {

		attrSpec := testmodel.NewList()
		r := func(id bson.ObjectId) (bson.M, error) {
			return bson.M{"name": "bob", "age": 245}, nil
		}

		pipe, err := makePipeline(
			attrSpec,
			r,
			bson.D{bson.DocElem{Name: "shard", Value: "good"}},
			bson.D{bson.DocElem{Name: "namespace", Value: "good"}},
			bson.D{bson.DocElem{Name: "forced", Value: "good"}},
			bson.D{bson.DocElem{Name: "user", Value: "good"}},
			[]string{"name", "-age"},
			"oh-no",
			2,
			[]string{"a", "b"},
		)
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, "Unable to execute query: after 'oh-no' is not parsable objectId")
		So(pipe, ShouldBeNil)
	})

	Convey("calling make pipeline with retriever returning an error format work", t, func() {

		attrSpec := testmodel.NewList()
		r := func(id bson.ObjectId) (bson.M, error) {
			return nil, fmt.Errorf("oh noes")
		}

		pipe, err := makePipeline(
			attrSpec,
			r,
			bson.D{bson.DocElem{Name: "shard", Value: "good"}},
			bson.D{bson.DocElem{Name: "namespace", Value: "good"}},
			bson.D{bson.DocElem{Name: "forced", Value: "good"}},
			bson.D{bson.DocElem{Name: "user", Value: "good"}},
			[]string{"name", "-age"},
			"673f8580686b0ea7a1241fee",
			2,
			[]string{"a", "b"},
		)
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, "Unable to execute query: unable to retrieve previous object with after id '673f8580686b0ea7a1241fee': oh noes")
		So(pipe, ShouldBeNil)
	})

	Convey("calling make pipeline with retriever returning a nil previous work", t, func() {

		attrSpec := testmodel.NewList()
		r := func(id bson.ObjectId) (bson.M, error) {
			return nil, nil
		}

		pipe, err := makePipeline(
			attrSpec,
			r,
			bson.D{bson.DocElem{Name: "shard", Value: "good"}},
			bson.D{bson.DocElem{Name: "namespace", Value: "good"}},
			bson.D{bson.DocElem{Name: "forced", Value: "good"}},
			bson.D{bson.DocElem{Name: "user", Value: "good"}},
			[]string{"name", "-age"},
			"673f8580686b0ea7a1241fee",
			2,
			[]string{"a", "b"},
		)
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, "Unable to execute query: unable to retrieve previous object with after id '673f8580686b0ea7a1241fee': not found")
		So(pipe, ShouldBeNil)
	})

}

func Test_makeFieldsSelector(t *testing.T) {
	type args struct {
		fields    []string
		setupSpec func(t *testing.T, ctrl *gomock.Controller) elemental.AttributeSpecifiable
	}
	tests := []struct {
		name string
		args args
		want bson.M
	}{
		{
			"simple",
			args{
				[]string{"MyField1", "myfield2", ""},
				nil,
			},
			bson.M{
				"myfield1": 1,
				"myfield2": 1,
			},
		},
		{
			"ID",
			args{
				[]string{"ID"},
				nil,
			},
			bson.M{
				"_id": 1,
			},
		},
		{
			"id",
			args{
				[]string{"ID"},
				nil,
			},
			bson.M{
				"_id": 1,
			},
		},
		{
			"inverted",
			args{
				[]string{"-something"},
				nil,
			},
			bson.M{
				"something": 1,
			},
		},
		{
			"empty",
			args{
				[]string{},
				nil,
			},
			nil,
		},
		{
			"nil",
			args{
				nil,
				nil,
			},
			nil,
		},
		{
			"only empty",
			args{
				[]string{"", ""},
				nil,
			},
			nil,
		},
		{
			"translate fields from provided spec - entry found",
			args{
				fields: []string{"FieldA"},
				setupSpec: func(t *testing.T, ctrl *gomock.Controller) elemental.AttributeSpecifiable {

					spec := internal.NewMockAttributeSpecifiable(ctrl)
					spec.
						EXPECT().
						SpecificationForAttribute("fielda").
						Return(
							elemental.AttributeSpecification{
								BSONFieldName: "a",
							},
						)

					return spec
				},
			},
			bson.M{
				"a": 1,
			},
		},
		{
			"translate fields from provided spec - no entry found - should default to whatever was provided",
			args{
				fields: []string{"FieldA"},
				setupSpec: func(t *testing.T, ctrl *gomock.Controller) elemental.AttributeSpecifiable {

					spec := internal.NewMockAttributeSpecifiable(ctrl)
					spec.
						EXPECT().
						SpecificationForAttribute("fielda").
						Return(
							elemental.AttributeSpecification{

								// notice how no entry was found for 'fielda' therefore the value in the filter will be used.

								BSONFieldName: "",
							},
						)

					return spec
				},
			},
			bson.M{
				"fielda": 1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			var spec elemental.AttributeSpecifiable
			if tt.args.setupSpec != nil {
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				spec = tt.args.setupSpec(t, ctrl)
			}

			if got := makeFieldsSelector(tt.args.fields, spec); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("makeFieldsSelector() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_applyOrdering(t *testing.T) {
	type args struct {
		order     []string
		setupSpec func(t *testing.T, ctrl *gomock.Controller) elemental.AttributeSpecifiable
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "simple",
			args: args{
				order:     []string{"NAME", "toto", ""},
				setupSpec: nil,
			},
			want: []string{"name", "toto"},
		},

		{
			name: "ID",
			args: args{
				order:     []string{"ID"},
				setupSpec: nil,
			},
			want: []string{"_id"},
		},
		{
			name: "-ID",
			args: args{
				order:     []string{"-ID"},
				setupSpec: nil,
			},
			want: []string{"-_id"},
		},

		{
			name: "id",
			args: args{
				order:     []string{"id"},
				setupSpec: nil,
			},
			want: []string{"_id"},
		},
		{
			name: "-id",
			args: args{
				order:     []string{"-id"},
				setupSpec: nil,
			},
			want: []string{"-_id"},
		},

		{
			name: "_id",
			args: args{
				order:     []string{"_id"},
				setupSpec: nil,
			},
			want: []string{"_id"},
		},

		{
			name: "only empty",
			args: args{
				order:     []string{"", ""},
				setupSpec: nil,
			},
			want: []string{},
		},

		{
			name: "only empty",
			args: args{
				order:     []string{"", ""},
				setupSpec: nil,
			},
			want: []string{},
		},

		{
			name: "translate order keys from spec",
			args: args{
				order: []string{
					"FieldA",
					"FieldB",
				},
				setupSpec: func(t *testing.T, ctrl *gomock.Controller) elemental.AttributeSpecifiable {

					spec := internal.NewMockAttributeSpecifiable(ctrl)
					spec.
						EXPECT().
						SpecificationForAttribute("FieldA").
						Return(
							elemental.AttributeSpecification{
								BSONFieldName: "a",
							},
						)
					spec.
						EXPECT().
						SpecificationForAttribute("FieldB").
						Return(
							elemental.AttributeSpecification{
								BSONFieldName: "b",
							},
						)

					return spec
				},
			},
			want: []string{"a", "b"},
		},

		{
			name: "translate order keys from spec w/ order prefix - one field",
			args: args{
				order: []string{
					"-FieldA",
					"FieldB",
				},
				setupSpec: func(t *testing.T, ctrl *gomock.Controller) elemental.AttributeSpecifiable {

					spec := internal.NewMockAttributeSpecifiable(ctrl)
					spec.
						EXPECT().
						SpecificationForAttribute("FieldA").
						Return(
							elemental.AttributeSpecification{
								BSONFieldName: "a",
							},
						)
					spec.
						EXPECT().
						SpecificationForAttribute("FieldB").
						Return(
							elemental.AttributeSpecification{
								BSONFieldName: "b",
							},
						)

					return spec
				},
			},
			want: []string{"-a", "b"},
		},

		{
			name: "translate order keys from spec - ID field - upper case",
			args: args{
				order: []string{
					"ID",
				},
				setupSpec: func(t *testing.T, ctrl *gomock.Controller) elemental.AttributeSpecifiable {

					spec := internal.NewMockAttributeSpecifiable(ctrl)
					spec.
						EXPECT().
						SpecificationForAttribute("ID").
						Return(
							elemental.AttributeSpecification{
								BSONFieldName: "_id",
							},
						)
					return spec
				},
			},
			want: []string{"_id"},
		},

		{
			name: "translate order keys from spec - ID field - lower case",
			args: args{
				order: []string{
					"id",
				},
				setupSpec: func(t *testing.T, ctrl *gomock.Controller) elemental.AttributeSpecifiable {

					spec := internal.NewMockAttributeSpecifiable(ctrl)
					spec.
						EXPECT().
						SpecificationForAttribute("id").
						Return(
							elemental.AttributeSpecification{
								BSONFieldName: "_id",
							},
						)
					return spec
				},
			},
			want: []string{"_id"},
		},

		{
			name: "translate order keys from spec - ID field - upper case - desc",
			args: args{
				order: []string{
					"-ID",
				},
				setupSpec: func(t *testing.T, ctrl *gomock.Controller) elemental.AttributeSpecifiable {

					spec := internal.NewMockAttributeSpecifiable(ctrl)
					spec.
						EXPECT().
						SpecificationForAttribute("ID").
						Return(
							elemental.AttributeSpecification{
								BSONFieldName: "_id",
							},
						)
					return spec
				},
			},
			want: []string{"-_id"},
		},

		{
			name: "translate order keys from spec - ID field - lower case - desc",
			args: args{
				order: []string{
					"-id",
				},
				setupSpec: func(t *testing.T, ctrl *gomock.Controller) elemental.AttributeSpecifiable {

					spec := internal.NewMockAttributeSpecifiable(ctrl)
					spec.
						EXPECT().
						SpecificationForAttribute("id").
						Return(
							elemental.AttributeSpecification{
								BSONFieldName: "_id",
							},
						)
					return spec
				},
			},
			want: []string{"-_id"},
		},

		{
			name: "translate order keys from spec w/ order prefix - both fields",
			args: args{
				order: []string{
					"-FieldA",
					"-FieldB",
				},
				setupSpec: func(t *testing.T, ctrl *gomock.Controller) elemental.AttributeSpecifiable {

					spec := internal.NewMockAttributeSpecifiable(ctrl)
					spec.
						EXPECT().
						SpecificationForAttribute("FieldA").
						Return(
							elemental.AttributeSpecification{
								BSONFieldName: "a",
							},
						)
					spec.
						EXPECT().
						SpecificationForAttribute("FieldB").
						Return(
							elemental.AttributeSpecification{
								BSONFieldName: "b",
							},
						)

					return spec
				},
			},
			want: []string{"-a", "-b"},
		},

		{
			name: "translate order keys from spec - default to provided value if nothing found in spec",
			args: args{
				order: []string{
					"YOLO",
				},
				setupSpec: func(t *testing.T, ctrl *gomock.Controller) elemental.AttributeSpecifiable {

					spec := internal.NewMockAttributeSpecifiable(ctrl)
					spec.
						EXPECT().
						SpecificationForAttribute("YOLO").
						Return(
							elemental.AttributeSpecification{
								BSONFieldName: "",
							},
						)

					return spec
				},
			},
			want: []string{"yolo"},
		},

		{
			name: "only empty",
			args: args{
				order:     []string{"", ""},
				setupSpec: nil,
			},
			want: []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			var spec elemental.AttributeSpecifiable
			if tt.args.setupSpec != nil {
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				spec = tt.args.setupSpec(t, ctrl)
			}

			if got := applyOrdering(tt.args.order, spec); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("applyOrdering() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_convertReadConsistency(t *testing.T) {
	type args struct {
		c manipulate.ReadConsistency
	}
	tests := []struct {
		name string
		args args
		want mgo.Mode
	}{
		{
			"eventual",
			args{manipulate.ReadConsistencyEventual},
			mgo.Eventual,
		},
		{
			"monotonic",
			args{manipulate.ReadConsistencyMonotonic},
			mgo.Monotonic,
		},
		{
			"nearest",
			args{manipulate.ReadConsistencyNearest},
			mgo.Nearest,
		},
		{
			"strong",
			args{manipulate.ReadConsistencyStrong},
			mgo.Strong,
		},
		{
			"weakest",
			args{manipulate.ReadConsistencyWeakest},
			mgo.SecondaryPreferred,
		},
		{
			"default",
			args{manipulate.ReadConsistencyDefault},
			-1,
		},
		{
			"something else",
			args{manipulate.ReadConsistency("else")},
			-1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := convertReadConsistency(tt.args.c); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("convertConsistency() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_convertWriteConsistency(t *testing.T) {
	type args struct {
		c manipulate.WriteConsistency
	}
	tests := []struct {
		name string
		args args
		want *mgo.Safe
	}{
		{
			"none",
			args{manipulate.WriteConsistencyNone},
			nil,
		},
		{
			"strong",
			args{manipulate.WriteConsistencyStrong},
			&mgo.Safe{WMode: "majority"},
		},
		{
			"strongest",
			args{manipulate.WriteConsistencyStrongest},
			&mgo.Safe{WMode: "majority", J: true},
		},
		{
			"default",
			args{manipulate.WriteConsistencyDefault},
			&mgo.Safe{},
		},
		{
			"something else",
			args{manipulate.WriteConsistency("else")},
			&mgo.Safe{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := convertWriteConsistency(tt.args.c); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("convertWriteConsistency() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_isConnectionError(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			"nil",
			args{
				nil,
			},
			false,
		},
		{
			"lost connection to server",
			args{
				fmt.Errorf("lost connection to server"),
			},
			true,
		},
		{
			"no reachable servers",
			args{
				fmt.Errorf("no reachable servers"),
			},
			true,
		},
		{
			"waiting for replication timed out",
			args{
				fmt.Errorf("waiting for replication timed out"),
			},
			true,
		},
		{
			"could not contact primary for replica set",
			args{
				fmt.Errorf("could not contact primary for replica set"),
			},
			true,
		},
		{
			"write results unavailable from",
			args{
				fmt.Errorf("write results unavailable from"),
			},
			true,
		},
		{
			`could not find host matching read preference { mode: "primary"`,
			args{
				fmt.Errorf(`could not find host matching read preference { mode: "primary"`),
			},
			true,
		},
		{
			"unable to target",
			args{
				fmt.Errorf("unable to target"),
			},
			true,
		},
		{
			"Connection refused",
			args{
				fmt.Errorf("blah: connection refused"),
			},
			true,
		},
		{
			"EOF",
			args{
				io.EOF,
			},
			true,
		},
		{
			"nope",
			args{
				fmt.Errorf("hey"),
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isConnectionError(tt.args.err); got != tt.want {
				t.Errorf("isConnectionError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getErrorCode(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			"*mgo.QueryError",
			args{
				&mgo.QueryError{Code: 42},
			},
			42,
		},
		{
			"*mgo.LastError",
			args{
				&mgo.LastError{Code: 42},
			},
			42,
		},

		{
			"*mgo.BulkError",
			args{
				&mgo.BulkError{ /* private */ },
			},
			0, // Should be 42. but that is sadly untestable... or is it?
		},
		{
			"",
			args{
				fmt.Errorf("yo"),
			},
			0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getErrorCode(tt.args.err); got != tt.want {
				t.Errorf("getErrorCode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_invalidQuery(t *testing.T) {

	type args struct {
		err error
	}

	testCases := map[string]struct {
		input   args
		wantOk  bool
		wantErr error
	}{
		"code 2 - bad regex": {
			input: args{
				err: &mgo.QueryError{
					Code:    2,
					Message: errInvalidQueryBadRegex,
				},
			},
			wantOk: true,
			wantErr: manipulate.ErrInvalidQuery{
				DueToFilter: true,
				Err: &mgo.QueryError{
					Code:    2,
					Message: errInvalidQueryBadRegex,
				},
			},
		},
		"code 51091 - invalid regex": {
			input: args{
				err: &mgo.QueryError{
					Code:    51091,
					Message: errInvalidQueryInvalidRegex,
				},
			},
			wantOk: true,
			wantErr: manipulate.ErrInvalidQuery{
				DueToFilter: true,
				Err: &mgo.QueryError{
					Code:    51091,
					Message: errInvalidQueryInvalidRegex,
				},
			},
		},
		"nil": {
			input: args{
				err: nil,
			},
			wantOk:  false,
			wantErr: nil,
		},
		"not an invalid query error": {
			input: args{
				err: errors.New("some other error"),
			},
			wantOk:  false,
			wantErr: nil,
		},
	}

	for scenario, tc := range testCases {
		t.Run(scenario, func(t *testing.T) {
			ok, err := invalidQuery(tc.input.err)

			if ok != tc.wantOk {
				t.Errorf("wanted '%t', got '%t'", tc.wantOk, ok)
			}

			if ok && err == nil {
				t.Error("no error was returned when one was expected")
			}

			if !reflect.DeepEqual(err, tc.wantErr) {
				t.Log("Error types did not match")
				t.Errorf("\n"+
					"EXPECTED:\n"+
					"%+v\n"+
					"ACTUAL:\n"+
					"%+v",
					tc.wantErr,
					err,
				)
			}
		})
	}
}

func Test_explainIfNeeded(t *testing.T) {

	identity := elemental.MakeIdentity("thing", "things")

	type args struct {
		query      *mgo.Query
		filter     bson.D
		identity   elemental.Identity
		operation  elemental.Operation
		explainMap map[elemental.Identity]map[elemental.Operation]struct{}
	}
	tests := []struct {
		name     string
		args     args
		wantFunc bool
	}{
		{
			"empty",
			args{
				nil,
				nil,
				identity,
				elemental.OperationCreate,
				map[elemental.Identity]map[elemental.Operation]struct{}{},
			},
			false,
		},
		{
			"nil",
			args{
				nil,
				nil,
				identity,
				elemental.OperationCreate,
				nil,
			},
			false,
		},
		{
			"matching exactly",
			args{
				nil,
				nil,
				identity,
				elemental.OperationCreate,
				map[elemental.Identity]map[elemental.Operation]struct{}{
					identity: {elemental.OperationCreate: {}},
				},
			},
			true,
		},
		{
			"matching with no operation",
			args{
				nil,
				nil,
				identity,
				elemental.OperationCreate,
				map[elemental.Identity]map[elemental.Operation]struct{}{
					identity: {},
				},
			},
			true,
		},
		{
			"matching with nil operation",
			args{
				nil,
				nil,
				identity,
				elemental.OperationCreate,
				map[elemental.Identity]map[elemental.Operation]struct{}{
					identity: nil,
				},
			},
			true,
		},
		{
			"not matching",
			args{
				nil,
				nil,
				identity,
				elemental.OperationCreate,
				map[elemental.Identity]map[elemental.Operation]struct{}{
					elemental.MakeIdentity("hello", "hellos"): {},
				},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := explainIfNeeded(tt.args.query, tt.args.filter, tt.args.identity, tt.args.operation, tt.args.explainMap); (got != nil) != tt.wantFunc {
				t.Errorf("explainIfNeeded() = %v, want %v", (got != nil), tt.wantFunc)
			}
		})
	}
}

func TestSetMaxTime(t *testing.T) {

	Convey("Calling setMaxTime with a context with no deadline should work", t, func() {
		q := &mgo.Query{}
		q, err := setMaxTime(context.Background(), q)
		So(err, ShouldBeNil)
		qr := (&mgo.Query{}).SetMaxTime(defaultGlobalContextTimeout)
		So(q, ShouldResemble, qr)
	})

	Convey("Calling setMaxTime with a context with valid deadline should work", t, func() {
		q := &mgo.Query{}
		deadline := time.Now().Add(3 * time.Second)
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		defer cancel()

		q, err := setMaxTime(ctx, q)
		So(err, ShouldBeNil)
		qr := (&mgo.Query{}).SetMaxTime(time.Until(deadline))
		So(q, ShouldResemble, qr)
	})

	Convey("Calling setMaxTime with a context with expired deadline should not work", t, func() {
		q := &mgo.Query{}
		deadline := time.Now().Add(-3 * time.Second)
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		defer cancel()

		q, err := setMaxTime(ctx, q)
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, "Unable to build query: context deadline exceeded")
		So(q, ShouldBeNil)
	})

	Convey("Calling setMaxTime with a canceled context should not work", t, func() {
		q := &mgo.Query{}
		deadline := time.Now().Add(3 * time.Second)
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		cancel()

		q, err := setMaxTime(ctx, q)
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, "Unable to build query: context canceled")
		So(q, ShouldBeNil)
	})
}
