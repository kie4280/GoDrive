package watcher

import (
	"github.com/fsnotify/fsnotify"
	"github.com/oleiade/lane"
	"godrive/internal/settings"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"time"
)

// LocalWatcher is the object returned by RegfsWatcher
type LocalWatcher struct {
	lastSync     time.Time
	userID       string
	localRoot    string
	watcher      *fsnotify.Watcher
	canRun       bool
	isRunning    bool
	settingStore settings.DriveConfig
	globalError  error
	changes      map[string]*fsnotify.Event
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

	lw.isRunning = false
	lw.userID = id
	lw.changes = make(map[string]*fsnotify.Event, localChangeListSize)
	lw.lastSync = time.Now()
	lw.settingStore, err = settings.ReadDriveConfig()
	if err != nil {
		return nil, err
	}
	user, err := lw.settingStore.GetUser(id)
	if err != nil {
		return nil, err
	}
	lw.localRoot = user.GetLocalRoot()

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
	queue := lane.NewQueue()
	queue.Enqueue(lw.localRoot)
	lw.watcher.Add(lw.localRoot)
	for !queue.Empty() {
		abspath := queue.Dequeue().(string)
		files, err := ioutil.ReadDir(abspath)
		checkErr(err)
		for _, i := range files {
			if i.IsDir() {
				fold := filepath.Join(abspath, i.Name())
				queue.Enqueue(fold)
				lw.watcher.Add(fold)
			}
		}

	}
	lw.canRun = true
	for lw.canRun {
		select {
		case event, ok := <-lw.watcher.Events:
			if !ok {
				return
			}
			log.Println("change file:", event.String())
			if event.Op&fsnotify.Create == fsnotify.Create {
				log.Println("create file:", event.Name)
				info, err := os.Stat(event.Name)
				checkErr(err)
				if info.IsDir() {
					lw.watcher.Add(event.Name)
				}
			}
		case err, ok := <-lw.watcher.Errors:
			if !ok {
				return
			}
			lw.globalError = err
			log.Println("error:", err)
		}
	}
}

// GetLocalChanges gets changes since the last call to GetLocalChanges
func (lw *LocalWatcher) GetLocalChanges() ([]*FileChange, error) {
	if lw.globalError != nil {
		return nil, lw.globalError
	}
	changes := make([]*FileChange, len(lw.changes))
	// copy(changes, lw.changeList)
	// lw.changeList = make([]*FileChange, localChangeListSize)
	return changes, nil
}

// Close all resources
func (lw *LocalWatcher) Close() {
	defer lw.watcher.Close()
	lw.canRun = false
}
