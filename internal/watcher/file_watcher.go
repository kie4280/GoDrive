package watcher

import (
	fsnotify "github.com/fsnotify/fsnotify"
	"time"
)

// LocalWatcher is the object returned by RegfsWatcher
type LocalWatcher struct {
	lastSync time.Time
	userID   int
	watcher  *fsnotify.Watcher
}

// RegfsWatcher register a watcher on local fs
func RegfsWatcher(id int) (*LocalWatcher, error) {
	lw := new(LocalWatcher)
	var errN error
	lw.watcher, errN = fsnotify.NewWatcher()

	return lw, errN
}
