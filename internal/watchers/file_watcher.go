package filewatcher

import (
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"os"
	"runtime/debug"
)

// FileWatcher indexes files
type FileWatcher struct {
	errorChan chan error
}

type watcherErrorInterface interface {
	Error() string
}

type watcherError struct {
	errstring string
}

func (we watcherError) Error() string {
	return we.errstring
}

func (fw *FileWatcher) onError(err error) {
	if err != nil {
		var we watcherError = watcherError{errstring: "FileWatcher error: " + err.Error() + string(debug.Stack())}
		fw.errorChan <- we
	}

}

func hashsum(file string) {
	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%x", h.Sum(nil))
}
