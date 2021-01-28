package utils

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
)

/*
This is the error section. This section contains the functions to
create wrapped errors.
*/

// GDriveError error
type GDriveError struct {
	outerr error
	inerr  error
}

// Error returns string error
func (ge *GDriveError) Error() string {
	return ge.outerr.Error()
}

// Unwrap returns inner error
func (ge *GDriveError) Unwrap() error {
	return ge.inerr
}

// Is returns true if the error contains "target"
func (ge *GDriveError) Is(target error) bool {
	return ge.outerr.Error() == target.Error()
}

// NewError creates a new wrapped error
func NewError(outer error, inner error) error {
	aa := &GDriveError{outerr: outer, inerr: inner}
	return aa
}

/*
This is the utility section, containing all kinds of utilities.
*/

// StringToMd5 gets the id of a particular account name
func StringToMd5(str string) string {
	hash := md5.New()
	io.WriteString(hash, str)
	id := hex.EncodeToString(hash.Sum(nil))
	return id
}

// CheckSum generates the md5 sum for "file"
func CheckSum(file string) (string, error) {
	abspath := filepath.Clean(file)
	f, openerr := os.Open(abspath)
	defer f.Close()
	if openerr != nil {
		return "", openerr
	}

	h1 := md5.New()
	_, copyerr := io.Copy(h1, f)
	if copyerr != nil {
		return "", copyerr
	}

	return hex.EncodeToString(h1.Sum(nil)), nil

}

/*
Below is a simple implementation for a read write mutex.

*/

var (
	// ErrInUse the lock is in use by writer
	ErrInUse error = errors.New("RWlock is in use")
	// ErrNotLocked the lock is not previously locked
	ErrNotLocked = errors.New("RWlock is not previously locked")
)

// RWMutex is a mutex
type RWMutex struct {
	accessCount int32
	condV       *sync.Cond
}

func newRWMutex() *RWMutex {
	rm := new(RWMutex)
	rm.accessCount = 0
	rm.condV = sync.NewCond(&sync.Mutex{})
	return rm
}

// RLock locks for read only
// args: (blocking) blocking: if set to true block on resource busy
// else return ErrInUse
func (rm *RWMutex) RLock(blocking bool) error {
	rm.condV.L.Lock()
	for rm.accessCount < 0 {
		if !blocking {
			rm.condV.L.Unlock()
			return ErrInUse
		}
		rm.condV.Wait()
	}
	rm.accessCount++
	rm.condV.L.Unlock()
	return nil
}

// RUnlock unlocks read only lock
func (rm *RWMutex) RUnlock() {
	rm.condV.L.Lock()
	defer rm.condV.L.Unlock()
	if rm.accessCount > 1 {
		rm.accessCount--

	} else if rm.accessCount == 1 {
		rm.condV.Broadcast()
	} else {
		panic(ErrNotLocked)
	}
}

// Lock locks for read write
func (rm *RWMutex) Lock(blocking bool) error {
	rm.condV.L.Lock()
	for rm.accessCount != 0 {
		if !blocking {
			rm.condV.L.Unlock()
			return ErrInUse
		}
		rm.condV.Wait()
	}
	rm.accessCount = -1
	rm.condV.L.Unlock()

	return nil
}

// Unlock unlocks the read write lock
func (rm *RWMutex) Unlock() {
	rm.condV.L.Lock()
	defer rm.condV.L.Unlock()
	if rm.accessCount >= 0 {
		panic(ErrNotLocked)
	} else {
		rm.accessCount = 0
	}
}
