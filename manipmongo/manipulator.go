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
	"fmt"
	"time"

	"go.acuvity.ai/manipulate"
)

const defaultGlobalContextTimeout = 60 * time.Second

// New returns a new manipulator backed by the official mongo driver.
func New(url string, db string, options ...Option) (manipulate.TransactionalManipulator, error) {
	return newMongo(url, db, options...)
}

type countRes struct {
	Count int `bson:"_count"`
}

func countFromResults(res []*countRes) (int, error) {
	switch len(res) {
	case 0:
		return 0, nil
	case 1:
		return res[0].Count, nil
	default:
		return 0, manipulate.ErrCannotExecuteQuery{Err: fmt.Errorf("count: invalid count result len: %d. must be 1", len(res))}
	}
}
