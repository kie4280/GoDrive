package watcher

import (
	"errors"
	"godrive/internal/settings"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	// "syscall"
	// "runtime"
	"time"
)

const (
	// Moved file has only been moved or renamed
	Moved int8 = 1
	// Removed file has been removed
	Removed int8 = 2
	// Modified file has only been written to or created
	Modified int8 = 3
	// Created file is created
	Created int8 = 4
)

const ()

var (
	// ErrChildBeforeParent this shouldn't normally happen
	ErrChildBeforeParent = errors.New("children created before parent")
)

const (
	defaultPollingInterv int = 10
)

// LocalWatcher is the object returned by RegfsWatcher
type LocalWatcher struct {
	lastSync      time.Time
	userID        string
	localRoot     string
	canRun        bool
	isRunning     bool
	pollingInterv int
	user          settings.UserConfig
	globalError   error
	rootFold      *fileStruct
	checkLock     sync.Mutex
	fstructPool   sync.Pool
	prevFiles     map[string]*fileStruct
	prevFolds     map[string]*fileStruct
	newFiles      map[string]*fileStruct
	newFolds      map[string]*fileStruct
	changeItems   []*FileChange
}

// FileChange represents the change to a file
type FileChange struct {
	OldPath    string
	NewPath    string
	ChangeType int8
	IsDir      bool
}

type fileStruct struct {
	relpath string
	stat    os.FileInfo
}

var fstructPool = sync.Pool{New: func() interface{} {
	return new(fileStruct)
}}

// RegfsWatcher register a watcher on local fs
func RegfsWatcher(id string) (*LocalWatcher, error) {
	lw := new(LocalWatcher)
	var err error

	lw.isRunning = false
	lw.userID = id
	lw.pollingInterv = defaultPollingInterv
	lw.lastSync = time.Now()
	settingstore, err := settings.ReadDriveConfig()
	if err != nil {
		return nil, err
	}
	lw.user, err = settingstore.GetUser(id)
	if err != nil {
		return nil, err
	}
	lw.localRoot = lw.user.GetLocalRoot()

	lw.newFiles = make(map[string]*fileStruct)
	lw.newFolds = make(map[string]*fileStruct)
	lw.prevFiles = make(map[string]*fileStruct)
	lw.prevFolds = make(map[string]*fileStruct)
	lw.changeItems = make([]*FileChange, 0, 200)

	go func() {

		lw.canRun = true
		// initial snapshot of root folder
		lw.recurseFold("/")
		lw.createSnapshot()
		lw.start()
	}()
	return lw, nil
}

func (lw *LocalWatcher) onLocalError() {
	if err := recover(); err != nil {
		err1 := err.(error)
		lw.globalError = err1
		log.Println("error", err1)
	}
}

func (lw *LocalWatcher) start() {
	defer lw.onLocalError()
	prev := time.Now()
	// watcher main event loop
	for lw.canRun {
		if int(time.Now().Sub(prev).Truncate(time.Second).
			Seconds()) >= lw.pollingInterv {
			lw.check()
			prev = time.Now()

		} else {
			time.Sleep(1 * time.Second)
		}
	}
}

func (lw *LocalWatcher) recurseFold(ph string) {

	files, err := ioutil.ReadDir(path.Join(lw.localRoot, ph))
	checkErr(err)
	for _, i := range files {
		if !lw.canRun {
			return
		}
		childRel := path.Join(ph, i.Name())
		if i.IsDir() {
			ff := createFile(childRel, i)
			lw.newFolds[ph] = ff
			lw.recurseFold(childRel)
		} else {
			nf := createFile(childRel, i)
			lw.newFiles[childRel] = nf
		}
	}
}

func (lw *LocalWatcher) check() {
	lw.checkLock.Lock()
	defer lw.checkLock.Unlock()
	for _, v := range lw.newFiles {
		fstructPool.Put(v)
	}
	for _, v := range lw.newFolds {
		fstructPool.Put(v)
	}
	lw.newFiles = make(map[string]*fileStruct)
	lw.newFolds = make(map[string]*fileStruct)
	lw.recurseFold("/")

}

func (lw *LocalWatcher) createSnapshot() {
	for _, v := range lw.prevFiles {
		fstructPool.Put(v)
	}
	for _, v := range lw.prevFolds {
		fstructPool.Put(v)
	}
	lw.prevFiles = lw.newFiles
	lw.prevFolds = lw.newFolds

	lw.newFiles = make(map[string]*fileStruct)
	lw.newFolds = make(map[string]*fileStruct)

}

// getDiff returns [created, removed, moved, modified]
func (lw *LocalWatcher) getDiff() {
	createdMap := make(map[string]*fileStruct)
	removedMap := make(map[string]*fileStruct)
	lw.changeItems = lw.changeItems[:0]

	for cfiK, cfiV := range lw.newFiles {
		f, ok := lw.prevFiles[cfiK]
		if ok {
			sameModTime := f.stat.ModTime().Equal(cfiV.stat.ModTime())
			same, err := sameFile(f.stat, cfiV.stat)
			checkErr(err)
			if !sameModTime || !same {
				fc := new(FileChange)
				fc.ChangeType = Modified
				fc.IsDir = false
				fc.NewPath = cfiK
				fc.OldPath = fc.NewPath
				lw.changeItems = append(lw.changeItems, fc)
			}
		} else {
			createdMap[cfiK] = cfiV
		}
	}
	for pfiK, pfiV := range lw.prevFiles {
		if _, ok := lw.newFiles[pfiK]; !ok {
			removedMap[pfiK] = pfiV
		}
	}
	for ck, cv := range createdMap {
		for rk, rv := range removedMap {
			sameModTime := cv.stat.ModTime().Equal(rv.stat.ModTime())
			sameID, err := sameFile(cv.stat, rv.stat)
			checkErr(err)
			if sameID && sameModTime {
				delete(createdMap, ck)
				delete(removedMap, rk)
				fc := new(FileChange)
				fc.ChangeType = Moved
				fc.IsDir = false
				fc.NewPath = ck
				fc.OldPath = rk
				lw.changeItems = append(lw.changeItems, fc)
			}
		}
	}

	for ck := range createdMap {
		fc := new(FileChange)
		fc.ChangeType = Created
		fc.IsDir = false
		fc.NewPath = ck
		fc.OldPath = ""
		lw.changeItems = append(lw.changeItems, fc)
	}

	for rk := range removedMap {
		fc := new(FileChange)
		fc.ChangeType = Removed
		fc.IsDir = false
		fc.NewPath = ""
		fc.OldPath = rk
		lw.changeItems = append(lw.changeItems, fc)
	}
}

func (lw *LocalWatcher) unWatch(target string) {

}

func getPathList(p string) []string {
	pp := path.Clean(p)
	return strings.Split(pp, "/")[1:]
}

func createFile(relpath string, stat os.FileInfo) *fileStruct {
	nn := fstructPool.Get().(*fileStruct)
	nn.relpath = relpath
	nn.stat = stat
	return nn
}

// GetLocalChanges gets the changes since watcher registered or the
// last call to GetLocalChanges()
func (lw *LocalWatcher) GetLocalChanges() ([]*FileChange, error) {

	if lw.globalError != nil {
		return nil, lw.globalError
	}

	changes := make([]*FileChange, 0, localChangeListSize)
	// copy(changes, lw.changeList)
	// lw.changeList = make([]*FileChange, localChangeListSize)
	return changes, nil
}

// Close all resources
func (lw *LocalWatcher) Close() {

	lw.canRun = false
}
