package fswatcher

import (
	"godrive/internal/settings"
	"io/ioutil"
	"os"
	"path"
	"sync"
	// "syscall"
	// "runtime"
	"godrive/internal/localfs"
	wcom "godrive/internal/watcher"
	"time"
)

// LocalWatcher is the object returned by RegfsWatcher
type LocalWatcher struct {
	lastSync    time.Time
	userID      string
	localRoot   string
	canRun      bool
	ready       bool
	isRunning   bool
	startWait   sync.WaitGroup
	user        settings.UserConfigHandle
	globalError error
	rootFold    *fileStruct
	fstructPool sync.Pool
	commandChan chan int8
	replyChan   chan int8
	errChan     chan error
	newFiles    map[string]*fileStruct
	newFolds    map[string]*fileStruct
	changeList  []*FileChange
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
func RegfsWatcher(userid string) (*LocalWatcher, error) {
	lw := new(LocalWatcher)
	var err error

	lw.isRunning = true
	lw.ready = false
	lw.startWait.Add(1)
	lw.userID = userid

	settingstore, err := settings.ReadDriveConfig()
	if err != nil {
		return nil, err
	}
	lw.user, err = settingstore.GetUser(userid)
	if err != nil {
		return nil, err
	}
	lw.localRoot = lw.user.GetSyncRoot()
	lw.commandChan = make(chan int8, 1)
	lw.replyChan = make(chan int8, 5)
	lw.errChan = make(chan error, 1)

	lw.newFiles = make(map[string]*fileStruct)
	lw.newFolds = make(map[string]*fileStruct)
	lw.changeList = make([]*FileChange, 0, wcom.LocalChangeListSize)

	go lw.start()
	return lw, nil
}

func (lw *LocalWatcher) onLocalError() {
	if err := recover(); err != nil {
		err1 := err.(error)
		lw.errChan <- err1
		lw.isRunning = false
		lw.canRun = false
		lw.ready = false

	}
}

// watcher main event loop
func (lw *LocalWatcher) start() {
	defer lw.onLocalError()
	defer func() {
		lw.isRunning = false
		lw.ready = false
	}()
	lw.canRun = true
	// initial snapshot of root folder
	store, err := localfs.Store(lw.userID)
	checkErr(err)
	if !store.Exist() {
		lw.initializeDB()
	}
	lw.ready = true
	lw.lastSync = time.Now()
	lw.processRequest()
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
			lw.newFolds[childRel] = ff
			lw.recurseFold(childRel)
		} else {
			nf := createFile(childRel, i)
			lw.newFiles[childRel] = nf
		}
	}
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

func (lw *LocalWatcher) getDiff() {
	createdMap := make(map[string]*fileStruct)
	removedMap := make(map[string]*fileStruct)
	lw.changeList = lw.changeList[:0]

	for cfiK, cfiV := range lw.newFolds {
		if _, ok := lw.prevFolds[cfiK]; !ok {
			createdMap[cfiK] = cfiV
		}
	}

	for pfiK, pfiV := range lw.prevFolds {
		if _, ok := lw.newFolds[pfiK]; !ok {
			removedMap[pfiK] = pfiV
		}
	}

	for ck, cv := range createdMap {
		for rk, rv := range removedMap {

			sameID, err := sameFile(cv.stat, rv.stat)
			checkErr(err)
			if sameID {
				delete(createdMap, ck)
				delete(removedMap, rk)
				fc := new(FileChange)
				fc.ChangeType = wcom.Moved
				fc.IsDir = true
				fc.NewPath = ck
				fc.OldPath = rk
				lw.changeList = append(lw.changeList, fc)
			}
		}
	}

	for ck := range createdMap {
		fc := new(FileChange)
		fc.ChangeType = wcom.Created
		fc.IsDir = true
		fc.NewPath = ck
		fc.OldPath = ""
		lw.changeList = append(lw.changeList, fc)
	}

	for rk := range removedMap {
		fc := new(FileChange)
		fc.ChangeType = wcom.Removed
		fc.IsDir = true
		fc.NewPath = ""
		fc.OldPath = rk
		lw.changeList = append(lw.changeList, fc)
	}

	createdMap = make(map[string]*fileStruct)
	removedMap = make(map[string]*fileStruct)

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
				lw.changeList = append(lw.changeList, fc)
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
				lw.changeList = append(lw.changeList, fc)
			}
		}
	}

	for ck := range createdMap {
		fc := new(FileChange)
		fc.ChangeType = Created
		fc.IsDir = false
		fc.NewPath = ck
		fc.OldPath = ""
		lw.changeList = append(lw.changeList, fc)
	}

	for rk := range removedMap {
		fc := new(FileChange)
		fc.ChangeType = Removed
		fc.IsDir = false
		fc.NewPath = ""
		fc.OldPath = rk
		lw.changeList = append(lw.changeList, fc)
	}

}

func (lw *LocalWatcher) check() {
	lw.recurseFold("/")
	lw.getDiff()
	lw.createSnapshot()
	changes := make([]*FileChange, len(lw.changeList))
	copy(changes, lw.changeList)
	lw.changeChan <- changes

}

func (lw *LocalWatcher) syncDB() {

}

func (lw *LocalWatcher) initializeDB() {

}

func createFile(relpath string, stat os.FileInfo) *fileStruct {
	nn := fstructPool.Get().(*fileStruct)
	// nn := new(fileStruct)
	nn.relpath = relpath
	nn.stat = stat
	return nn
}

// ChangeChan returns the channel of changes made
func (lw *LocalWatcher) ChangeChan() <-chan []*FileChange {
	return lw.changeChan
}

// Error returns the error channel
func (lw *LocalWatcher) Error() <-chan error {
	return lw.errChan
}

// SendComd send the command to the local watcher
func (lw *LocalWatcher) SendComd(command int8) error {
	lw.startWait.Wait()
	if !lw.isRunning {
		return ErrTerminated
	}
	var sent bool = false
	start := time.Now()
	for time.Now().Sub(start).Milliseconds() <= 500 { // timeout is 500ms
		select {
		case lw.commandChan <- command:
			sent = true
		default:
			time.Sleep(20 * time.Millisecond)
		}
	}
	if !sent {
		return ErrJammed
	}
	start = time.Now()
	for time.Now().Sub(start).Milliseconds() <= 500 { // timeout is 500ms
		select {
		case response := <-lw.replyChan:

			switch response {

			case S_ACK:
			}

			return nil
		default:
			time.Sleep(20 * time.Millisecond)
		}
	}

	return ErrNoResponse

}

func (lw *LocalWatcher) processRequest() {
	lw.startWait.Done()
	for lw.isRunning {

		b := <-lw.commandChan
		switch b {
		case C_CANCEL:
			lw.canRun = false
			lw.replyChan <- S_ACK
		case C_GET_CHANGE:
			if lw.ready {
				lw.replyChan <- S_ACK
				go lw.check()
			} else {
				lw.replyChan <- S_NOT_READY
			}

		default: // unrecognized command. return command to chan
			lw.commandChan <- b
			time.Sleep(100 * time.Millisecond) // prevent further reading
		}

	}

}
