package utils

import (
	"crypto/md5"
	"encoding/hex"
	"io"
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

// GetMd5Sum gets the id of a particular account name
func GetMd5Sum(str string) string {
	hash := md5.New()
	io.WriteString(hash, str)
	id := hex.EncodeToString(hash.Sum(nil))
	return id
}
