// +build !windows

package watcher

import (
	"os"
)

// SameFile compares the files. Only works on unix
func sameFile(file1, file2 os.FileInfo) (bool, error) {

	return os.SameFile(file1, file2), nil
}
