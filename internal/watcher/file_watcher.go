package watcher

import (
	fsnotify "github.com/fsnotify/fsnotify"
	"godrive/internal/settings"
	"log"
	"time"
)

// LocalWatcher is the object returned by RegfsWatcher
type LocalWatcher struct {
	lastSync     time.Time
	userID       string
	watcher      *fsnotify.Watcher
	canRun       bool
	isRunning    bool
	settingStore *settings.DriveConfigs
	globalError  error
	changeList   []*FileChange
}

// FileChange represents the change to a file
type FileChange struct {
	path       string
	changeType string
}

// RegfsWatcher register a watcher on local fs
func RegfsWatcher(id string) (*LocalWatcher, error) {
	lw := new(LocalWatcher)
	var err error
	lw.watcher, err = fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	lw.canRun = true
	lw.isRunning = false
	lw.userID = id
	lw.changeList = make([]*FileChange, 0, localChangeListSize)
	lw.lastSync = time.Now()
	lw.settingStore, err = settings.ReadDriveConfig()
	if err != nil {
		return nil, err
	}

	go lw.startWatcher()
	return lw, nil
}

func (lw *LocalWatcher) onLocalError() {
	if err := recover(); err != nil {
		err1 := err.(error)
		lw.globalError = err1
	}
}

func (lw *LocalWatcher) startWatcher() {
	defer lw.onLocalError()

	for {
		select {
		case event, ok := <-lw.watcher.Events:
			if !ok {
				return
			}
			log.Println("event:", event)
			if event.Op&fsnotify.Write == fsnotify.Write {
				log.Println("modified file:", event.Name)
			}
		case err, ok := <-lw.watcher.Errors:
			if !ok {
				return
			}
			log.Println("error:", err)
		}
	}
}

// GetLocalChanges gets changes since the last call to GetLocalChanges
func (lw *LocalWatcher) GetLocalChanges() ([]*FileChange, error) {
	if lw.globalError != nil {
		return nil, lw.globalError
	}
	changes := make([]*FileChange, len(lw.changeList))
	copy(changes, lw.changeList)
	lw.changeList = make([]*FileChange, localChangeListSize)
	return changes, nil
}

// Close all resources
func (lw *LocalWatcher) Close() {
	defer lw.watcher.Close()
}
