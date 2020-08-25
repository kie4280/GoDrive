package watcher

import (
	fsnotify "github.com/fsnotify/fsnotify"
	"log"
	"time"
)

// LocalWatcher is the object returned by RegfsWatcher
type LocalWatcher struct {
	lastSync  time.Time
	userID    string
	watcher   *fsnotify.Watcher
	canRun    bool
	isRunning bool
}

// RegfsWatcher register a watcher on local fs
func RegfsWatcher(id string) (*LocalWatcher, error) {
	lw := new(LocalWatcher)
	var errN error
	lw.watcher, errN = fsnotify.NewWatcher()
	lw.canRun = true
	lw.isRunning = false
	lw.userID = id
	lw.lastSync = time.Now()

	go lw.startWatcher()
	return lw, errN
}

// SendComd to the
func (lw *LocalWatcher) SendComd(comd int8) {

}

// Add a file to be watched
func (lw *LocalWatcher) add(path string) {

	done := make(chan bool)
	go func() {

	}()

	err := lw.watcher.Add(path)
	if err != nil {
		log.Fatal(err)
	}
	<-done
}

func (lw *LocalWatcher) getComd() {

}

func (lw *LocalWatcher) startWatcher() {

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

// Close all resources
func (lw *LocalWatcher) Close() {
	defer lw.watcher.Close()
}
