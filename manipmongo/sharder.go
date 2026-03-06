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
	"go.acuvity.ai/elemental"
	"go.acuvity.ai/manipulate"
	bson "go.mongodb.org/mongo-driver/v2/bson"
)

// Sharder defines sharding hooks for the official mongo-driver runtime.
type Sharder interface {
	Shard(manipulate.TransactionalManipulator, manipulate.Context, elemental.Identifiable) error
	OnShardedWrite(manipulate.TransactionalManipulator, manipulate.Context, elemental.Operation, elemental.Identifiable) error
	FilterOne(manipulate.TransactionalManipulator, manipulate.Context, elemental.Identifiable) (bson.D, error)
	FilterMany(manipulate.TransactionalManipulator, manipulate.Context, elemental.Identity) (bson.D, error)
}
