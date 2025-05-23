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

	"go.acuvity.ai/manipulate"
)

type config struct {
	upstreamManipulator   manipulate.Manipulator
	upstreamSubscriber    manipulate.Subscriber
	logfile               string
	enableLog             bool
	transactionQueue      chan *Transaction
	readConsistency       manipulate.ReadConsistency
	writeConsistency      manipulate.WriteConsistency
	defaultQueueDuration  time.Duration
	defaultPageSize       int
	prefetcher            Prefetcher
	upstreamReconciler    Reconciler
	downstreamReconciler  Reconciler
	disableUpstreamCommit bool
}

func newConfig() *config {
	return &config{
		transactionQueue:     make(chan *Transaction, 1000),
		readConsistency:      manipulate.ReadConsistencyEventual,
		writeConsistency:     manipulate.WriteConsistencyStrong,
		defaultQueueDuration: time.Second,
		defaultPageSize:      10000,
	}
}

// Option represents an option can can be passed to NewContext.
type Option func(*config)

// OptionDefaultConsistency sets the default read and write consistency.
func OptionDefaultConsistency(read manipulate.ReadConsistency, write manipulate.WriteConsistency) Option {
	return func(cfg *config) {
		if read != manipulate.ReadConsistencyDefault {
			cfg.readConsistency = read
		}
		if write != manipulate.WriteConsistencyDefault {
			cfg.writeConsistency = write
		}
	}
}

// OptionUpstreamManipulator sets the upstream manipulator.
func OptionUpstreamManipulator(manipulator manipulate.Manipulator) Option {
	return func(cfg *config) {
		cfg.upstreamManipulator = manipulator
	}
}

// OptionUpstreamSubscriber sets the upstream subscriber.
// Note the given subscriber must NOT be started or the events
// will be received twice, needlessly loading the VortexDB.
func OptionUpstreamSubscriber(s manipulate.Subscriber) Option {
	return func(cfg *config) {
		cfg.upstreamSubscriber = s
	}
}

// OptionTransactionLog sets the transaction log file.
func OptionTransactionLog(filename string) Option {
	return func(cfg *config) {
		cfg.logfile = filename
		cfg.enableLog = filename != ""
	}
}

// OptionTransactionQueueLength sets the queue length of the
// transaction queue.
func OptionTransactionQueueLength(n int) Option {
	return func(cfg *config) {
		cfg.transactionQueue = make(chan *Transaction, n)
	}
}

// OptionTransactionQueueDuration sets the default queue transaction
// duration. Once expired, the transaction is discarded.
func OptionTransactionQueueDuration(d time.Duration) Option {
	return func(cfg *config) {
		cfg.defaultQueueDuration = d
	}
}

// OptionDefaultPageSize is the page size during fetching.
func OptionDefaultPageSize(defaultPageSize int) Option {
	return func(cfg *config) {
		cfg.defaultPageSize = defaultPageSize
	}
}

// OptionPrefetcher sets the Prefetcher to use.
func OptionPrefetcher(p Prefetcher) Option {
	return func(cfg *config) {
		cfg.prefetcher = p
	}
}

// OptionDownstreamReconciler sets the global downstream Reconcilers to use.
func OptionDownstreamReconciler(r Reconciler) Option {
	return func(cfg *config) {
		cfg.downstreamReconciler = r
	}
}

// OptionUpstreamReconciler sets the global upstream Reconcilers to use.
func OptionUpstreamReconciler(r Reconciler) Option {
	return func(cfg *config) {
		cfg.upstreamReconciler = r
	}
}

// OptionDisableCommitUpstream sets the global upstream Reconcilers to use.
func OptionDisableCommitUpstream(disabled bool) Option {
	return func(cfg *config) {
		cfg.disableUpstreamCommit = disabled
	}
}
