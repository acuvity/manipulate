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

package manipvortex

import (
	"context"
	"fmt"
	"testing"

	"go.acuvity.ai/elemental"
	testmodel "go.acuvity.ai/elemental/test/model"
	"go.acuvity.ai/manipulate"

	. "github.com/smartystreets/goconvey/convey"
)

func TestTestPrefetcher(t *testing.T) {

	Convey("Given I create a new TestPrefetcher", t, func() {

		p := NewTestPrefetcher()

		Convey("Then it should be initialized", func() {
			So(p, ShouldImplement, (*TestPrefetcher)(nil))
			So(p.(*testPrefetcher).lock, ShouldNotBeNil)
			So(p.(*testPrefetcher).mocks, ShouldNotBeNil)
		})

		Convey("When I call Prefetch", func() {

			out, err := p.Prefetch(context.Background(), elemental.OperationRetrieve, elemental.EmptyIdentity, nil, nil)

			Convey("Then err should be nil", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then out should be nil", func() {
				So(out, ShouldBeNil)
			})
		})

		Convey("When I call WarmUp", func() {

			out, err := p.WarmUp(context.Background(), nil, nil, elemental.EmptyIdentity)

			Convey("Then err should be nil", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then out should be nil", func() {
				So(out, ShouldBeNil)
			})

		})

		Convey("When I call Flush", func() {

			p.Flush()

			Convey("Then nothing should happen", func() {})

		})

		Convey("When I mock and call Prefetch", func() {

			p.MockPrefetch(t, func(context.Context, elemental.Operation, elemental.Identity, manipulate.Manipulator, manipulate.Context) (elemental.Identifiables, error) {
				return testmodel.ListsList{}, fmt.Errorf("boom")
			})

			out, err := p.Prefetch(context.Background(), elemental.OperationRetrieve, elemental.EmptyIdentity, nil, nil)

			Convey("Then err should not be nil", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "boom")
			})

			Convey("Then out should not be nil", func() {
				So(out, ShouldNotBeNil)
			})
		})

		Convey("When I mock and call WarmUp", func() {

			p.MockWarmUp(t, func(context.Context, manipulate.Manipulator, elemental.ModelManager, elemental.Identity) (elemental.Identifiables, error) {
				return testmodel.ListsList{}, fmt.Errorf("boom")
			})

			out, err := p.WarmUp(context.Background(), nil, nil, elemental.EmptyIdentity)

			Convey("Then err should not be nil", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "boom")
			})

			Convey("Then out should not be nil", func() {
				So(out, ShouldNotBeNil)
			})
		})

		Convey("When I mock and call Flush", func() {

			p.MockFlush(t, func() { panic("boom") })

			Convey("The it should panic", func() {
				So(func() { p.Flush() }, ShouldPanicWith, "boom")
			})

		})
	})
}
