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
	"time"

	"go.acuvity.ai/elemental"
	"go.acuvity.ai/manipulate"
)

// RetrieveManyHook is the type of a hook for retrieve many
type RetrieveManyHook func(m manipulate.Manipulator, mctx manipulate.Context, dest elemental.Identifiables) (reconcile bool, err error)

// Processor configures the processing details for a specific identity.
type Processor struct {

	// Identity is the identity of the object that is stored in the DB.
	Identity elemental.Identity

	// TODO: Mode is the type of default consistency mode required from the cache.
	// This consistency can be overwritten by manipulate options.
	WriteConsistency manipulate.WriteConsistency

	// TODO: Mode is the type of default consistency mode required from the cache.
	// This consistency can be overwritten by manipulate options.
	ReadConsistency manipulate.ReadConsistency

	// QueueingDuration is the maximum time that an object should be
	// cached if the backend is not responding.
	QueueingDuration time.Duration

	// RetrieveManyHook is a hook function that can be called
	// before a RetrieveMany call. It returns an error and a continue
	// boolean. If the continue false, we can return without any
	// additional calls.
	RetrieveManyHook RetrieveManyHook

	// DownstreamReconciler is the custom Reconciler for the processor that
	// will be called to reconcile downstream writes.
	DownstreamReconciler Reconciler

	// UpstreamReconciler is the custom Reconciler for the processor that
	// will be called to reconcile upstream writes.
	UpstreamReconciler Reconciler

	// CommitOnEvent with commit the event in the cache even if a client
	// is subscribed for this event. If left false, it will only commit
	// if no client has subscribed for this event. It will always forward
	// the event to the clients.
	CommitOnEvent bool

	// LazySync will not sync all data of the identity on startup, but
	// will only load data on demand based on the transactions.
	LazySync bool
}
