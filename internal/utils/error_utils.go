package utils

// GDriveError error
type GDriveError struct {
	outerr error
	inerr  error
}

func (ge *GDriveError) Error() string {
	return ge.outerr.Error()
}

func (ge *GDriveError) Unwrap() error {
	return ge.inerr
}

func (ge *GDriveError) Is(target error) bool {
	return ge.outerr.Error() == target.Error()
}

// NewError creates a new wrapped error
func NewError(outer error, inner error) error {
	aa := &GDriveError{outerr: outer, inerr: inner}
	return aa
}
