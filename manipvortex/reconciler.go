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
	"sync"
	"testing"

	"go.acuvity.ai/elemental"
	"go.acuvity.ai/manipulate"
)

// An Reconciler can be given to manipvortex to perform
// pre write reconciliation.
type Reconciler interface {

	// Reconcile is called before a write operation to
	// to determine if the objects needs reconciliation. If it returns
	// false, the objects are ignored.
	// If it returns an error, the error will be forwarded to the caller.
	// The Reconcile function may modify the objects to perform transformations.
	Reconcile(manipulate.Context, elemental.Operation, elemental.Identifiable) (elemental.Identifiable, bool, error)
}

// A TestReconciler is an Reconciler that can be used for
// testing purposes.
type TestReconciler interface {
	Reconciler
	MockReconcile(t *testing.T, impl func(manipulate.Context, elemental.Operation, elemental.Identifiable) (elemental.Identifiable, bool, error))
}

type mockedReconcilerMethods struct {
	reconcileMock func(manipulate.Context, elemental.Operation, elemental.Identifiable) (elemental.Identifiable, bool, error)
}

type testReconciler struct {
	mocks       map[*testing.T]*mockedReconcilerMethods
	lock        *sync.Mutex
	currentTest *testing.T
}

// NewTestReconciler returns a new TestReconciler.
func NewTestReconciler() TestReconciler {
	return &testReconciler{
		lock:  &sync.Mutex{},
		mocks: map[*testing.T]*mockedReconcilerMethods{},
	}
}

// MockPrefetch sets the mocked implementation of Reconcile.
func (p *testReconciler) MockReconcile(t *testing.T, impl func(manipulate.Context, elemental.Operation, elemental.Identifiable) (elemental.Identifiable, bool, error)) {
	p.currentMocks(t).reconcileMock = impl
}

func (p *testReconciler) Reconcile(mctx manipulate.Context, op elemental.Operation, i elemental.Identifiable) (elemental.Identifiable, bool, error) {
	if mock := p.currentMocks(p.currentTest); mock != nil && mock.reconcileMock != nil {
		return mock.reconcileMock(mctx, op, i)
	}

	return i, true, nil
}

func (p *testReconciler) currentMocks(t *testing.T) *mockedReconcilerMethods {

	p.lock.Lock()
	defer p.lock.Unlock()

	mocks := p.mocks[t]

	if mocks == nil {
		mocks = &mockedReconcilerMethods{}
		p.mocks[t] = mocks
	}

	p.currentTest = t
	return mocks
}
