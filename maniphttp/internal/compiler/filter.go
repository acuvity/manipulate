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

package compiler

import (
	"net/url"

	"go.acuvity.ai/elemental"
)

// CompileFilter compiles the given filter into a http query filter.
func CompileFilter(f *elemental.Filter) (url.Values, error) {
	return url.Values{"q": []string{f.String()}}, nil
}
