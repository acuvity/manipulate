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

package maniptest

import (
	"context"
	"sync"
	"testing"

	"go.acuvity.ai/elemental"
	"go.acuvity.ai/manipulate"
)

type mockedSubscriberMethods struct {
	startMock       func(context.Context, *elemental.PushConfig)
	updateFilerMock func(*elemental.PushConfig)
	eventsMock      func() chan *elemental.Event
	errorsMock      func() chan error
	statusMock      func() chan manipulate.SubscriberStatus
}

// A TestSubscriber is the interface of mockable test manipulator.
type TestSubscriber interface {
	manipulate.Subscriber
	MockStart(t *testing.T, impl func(context.Context, *elemental.PushConfig))
	MockUpdateFilter(t *testing.T, impl func(*elemental.PushConfig))
	MockEvents(t *testing.T, impl func() chan *elemental.Event)
	MockErrors(t *testing.T, impl func() chan error)
	MockStatus(t *testing.T, impl func() chan manipulate.SubscriberStatus)
}

// A testSubscriber is an empty TransactionalManipulator that can be easily mocked.
type testSubscriber struct {
	mocks       map[*testing.T]*mockedSubscriberMethods
	lock        *sync.Mutex
	currentTest *testing.T
}

// NewTestSubscriber returns a new TestSubscriber.
func NewTestSubscriber() TestSubscriber {
	return &testSubscriber{
		lock:  &sync.Mutex{},
		mocks: map[*testing.T]*mockedSubscriberMethods{},
	}
}

func (m *testSubscriber) MockStart(t *testing.T, impl func(context.Context, *elemental.PushConfig)) {

	m.lock.Lock()
	defer m.lock.Unlock()

	m.currentMocks(t).startMock = impl
}

func (m *testSubscriber) MockUpdateFilter(t *testing.T, impl func(*elemental.PushConfig)) {

	m.lock.Lock()
	defer m.lock.Unlock()

	m.currentMocks(t).updateFilerMock = impl
}

func (m *testSubscriber) MockEvents(t *testing.T, impl func() chan *elemental.Event) {

	m.lock.Lock()
	defer m.lock.Unlock()

	m.currentMocks(t).eventsMock = impl
}

func (m *testSubscriber) MockErrors(t *testing.T, impl func() chan error) {

	m.lock.Lock()
	defer m.lock.Unlock()

	m.currentMocks(t).errorsMock = impl
}

func (m *testSubscriber) MockStatus(t *testing.T, impl func() chan manipulate.SubscriberStatus) {

	m.lock.Lock()
	defer m.lock.Unlock()

	m.currentMocks(t).statusMock = impl
}

func (m *testSubscriber) Start(ctx context.Context, filter *elemental.PushConfig) {

	m.lock.Lock()
	defer m.lock.Unlock()

	if mock := m.currentMocks(m.currentTest); mock != nil && mock.startMock != nil {
		mock.startMock(ctx, filter)
	}

}

func (m *testSubscriber) UpdateFilter(filter *elemental.PushConfig) {

	m.lock.Lock()
	defer m.lock.Unlock()

	if mock := m.currentMocks(m.currentTest); mock != nil && mock.updateFilerMock != nil {
		mock.updateFilerMock(filter)
	}

}

func (m *testSubscriber) Events() chan *elemental.Event {

	m.lock.Lock()
	defer m.lock.Unlock()

	if mock := m.currentMocks(m.currentTest); mock != nil && mock.eventsMock != nil {
		return mock.eventsMock()
	}

	return nil
}

func (m *testSubscriber) Errors() chan error {

	m.lock.Lock()
	defer m.lock.Unlock()

	if mock := m.currentMocks(m.currentTest); mock != nil && mock.errorsMock != nil {
		return mock.errorsMock()
	}

	return nil
}

func (m *testSubscriber) Status() chan manipulate.SubscriberStatus {

	m.lock.Lock()
	defer m.lock.Unlock()

	if mock := m.currentMocks(m.currentTest); mock != nil && mock.statusMock != nil {
		return mock.statusMock()
	}

	return nil
}

func (m *testSubscriber) currentMocks(t *testing.T) *mockedSubscriberMethods {

	mocks := m.mocks[t]

	if mocks == nil {
		mocks = &mockedSubscriberMethods{}
		m.mocks[t] = mocks
	}

	m.currentTest = t
	return mocks
}
