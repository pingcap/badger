// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// directio is a proxy package for github.com/pingcap/badger/directio
package directio

import (
	"errors"
	"os"

	"github.com/ncw/directio"
	"golang.org/x/sys/unix"
)

// OpenFile tries to open file in directio mode. If the file system doesn't support directio,
// it will fallback to open the file directly (without O_DIRECT)
func OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	file, err := directio.OpenFile(name, flag, perm)

	if errors.Is(err, unix.EINVAL) {
		return os.OpenFile(name, flag, perm)
	}

	return file, err
}
