// +build windows

/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package badger

// OpenDir opens a directory in windows with write access for syncing.
import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"unsafe"

	"github.com/pingcap/errors"
	"golang.org/x/sys/windows"
)

func openDir(path string) (*os.File, error) {
	fd, err := openDirWin(path)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), path), nil
}

func openDirWin(path string) (fd syscall.Handle, err error) {
	if len(path) == 0 {
		return syscall.InvalidHandle, syscall.ERROR_FILE_NOT_FOUND
	}
	pathp, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return syscall.InvalidHandle, err
	}
	access := uint32(syscall.GENERIC_READ | syscall.GENERIC_WRITE)
	sharemode := uint32(syscall.FILE_SHARE_READ | syscall.FILE_SHARE_WRITE)
	createmode := uint32(syscall.OPEN_EXISTING)
	fl := uint32(syscall.FILE_FLAG_BACKUP_SEMANTICS)
	return syscall.CreateFile(pathp, access, sharemode, nil, createmode, fl, 0)
}

// DirectoryLockGuard holds a lock on the directory.
type directoryLockGuard struct {
	fd *os.File
}

// AcquireDirectoryLock acquires exclusive access to a directory.
func acquireDirectoryLock(dirPath string, pidFileName string, readOnly bool) (*directoryLockGuard, error) {
	// Convert to absolute path so that Release still works even if we do an unbalanced
	// chdir in the meantime.
	absLockFilePath, err := filepath.Abs(filepath.Join(dirPath, pidFileName))
	if err != nil {
		return nil, errors.Wrap(err, "Cannot get absolute path for pid lock file")
	}

	file, err := createFileAndLock(absLockFilePath, readOnly, false)
	if err != nil {
		return nil, err
	}
	if file == nil {
		file, err = createFileAndLock(absLockFilePath, readOnly, true)
	}
	if err != nil {
		return nil, err
	}

	if !readOnly {
		_, err = file.WriteAt([]byte(fmt.Sprintf("%d\n", os.Getpid())), 0)
		if err != nil {
			_ = file.Close()
			return nil, errors.Wrap(err, "Cannot write to pid lock file")
		}
	}
	return &directoryLockGuard{fd: file}, nil
}

var (
	mod  = windows.NewLazyDLL("kernel32.dll")
	proc = mod.NewProc("CreateFileW")
)

func createFileAndLock(path string, share bool, create bool) (*os.File, error) {
	dwShareMode := uint32(0) // Exclusive (no sharing)
	if share {
		dwShareMode = windows.FILE_SHARE_READ
	}
	op := windows.OPEN_EXISTING
	if create {
		op = windows.CREATE_NEW
	}

	a, _, err := proc.Call(
		uintptr(unsafe.Pointer(windows.StringToUTF16Ptr(path))),
		uintptr(windows.GENERIC_READ|windows.GENERIC_WRITE),
		uintptr(dwShareMode),
		uintptr(0), // No security attributes.
		uintptr(op),
		uintptr(windows.FILE_ATTRIBUTE_NORMAL),
		0, // No template file.
	)
	switch err.(windows.Errno) {
	case 0:
		return os.NewFile(a, path), nil
	case windows.ERROR_FILE_NOT_FOUND:
		return nil, nil
	case windows.ERROR_SHARING_VIOLATION, windows.ERROR_FILE_EXISTS:
		return nil, errors.New(fmt.Sprintf("Cannot acquire directory lock on %q, Another process is using this Badger database.", path))
	default:
		_ = windows.Close(windows.Handle(a))
		return nil, errors.WithStack(err)
	}
}

// Release removes the directory lock.
func (g *directoryLockGuard) release() error {
	if g.fd == nil {
		return nil
	}
	return g.fd.Close()
}
