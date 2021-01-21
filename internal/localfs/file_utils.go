package localfs

import (

	// "errors"
	"github.com/gabriel-vasile/mimetype"
	"github.com/oleiade/lane"
	"github.com/panjf2000/ants/v2"
	"godrive/internal/settings"
	"godrive/internal/utils"
	"io/ioutil"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxGoroutine = 10
	minGoroutine = 2

	// C_CANCEL command to cancel current operation
	C_CANCEL int8 = 1
)

// LocalClient indexes files
type LocalClient struct {
	rootDir    string
	canRunList bool
	store      *LCStore
	userUD     string
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

// NewClient returns a new LocalClient object
func NewClient(userID string) (*LocalClient, error) {
	ws := new(LocalClient)
	setting, err := settings.ReadDriveConfig()
	if err != nil {
		return nil, err
	}
	user, err := setting.GetUser(userID)
	if err != nil {
		return nil, err
	}
	ws.rootDir = user.GetSyncRoot()
	ws.canRunList = false
	ws.store, err = NewStore(userID)
	if err != nil {
		return nil, err
	}
	return ws, nil
}

// Hashsum returns the md5 hash of "file" with path relative to rootDir
func (fw *LocalClient) hashsum(relpath string) string {

	abspath := filepath.Join(fw.rootDir, relpath)
	check, err := utils.CheckSum(abspath)
	checkErr(err)
	return check
}

// ListProgress of the command
type ListProgress struct {
	Files   int
	Folders int
	Done    bool
}

// ListHdl is the handle returned by ListAll
type ListHdl struct {
	progressChan    chan *ListProgress
	errChan         chan error
	commandChan     chan int8
	local           *LocalClient
	folderQueue     *lane.Queue
	foldQMux        sync.Mutex
	filecount       int32
	foldcount       int32
	onGoingRequests int32
	storeW          StoreWrite
}

// ListAll lists the folders and files below "location"
func (fw *LocalClient) ListAll() *ListHdl {
	lh := new(ListHdl)
	lh.progressChan = make(chan *ListProgress, 5)
	lh.commandChan = make(chan int8)
	lh.errChan = make(chan error, 5)
	lh.local = fw
	go lh.listAll()
	return lh
}

func (lh *ListHdl) listAll() {
	defer lh.onListError()
	p, err := ants.NewPoolWithFunc(maxGoroutine, lh.recursiveFoldsearch)
	checkErr(err)
	defer p.Release()
	lh.storeW, err = lh.local.store.AcquireWrite(true)
	checkErr(err)
	defer lh.storeW.Release()
	lh.onGoingRequests = 0
	lh.filecount, lh.foldcount = 0, 0
	lh.folderQueue = lane.NewQueue()
	lh.local.canRunList = true

	var workDone bool = false
	lh.folderQueue.Enqueue("/")
	progTimer := time.Now()

	for !workDone && lh.local.canRunList {

		if p.Free() > 0 && !lh.folderQueue.Empty() {
			atomic.AddInt32(&lh.onGoingRequests, 1)
			checkErr(p.Invoke([1]interface{}{lh.folderQueue.Dequeue()}))

		} else {
			time.Sleep(10 * time.Millisecond) // lighten load for CPU
		}

		workDone = lh.folderQueue.Empty() &&
			atomic.LoadInt32(&lh.onGoingRequests) == 0
		if time.Now().Sub(progTimer).Milliseconds() >= 1000 {
			if len(lh.progressChan) >= 4 {
				<-lh.progressChan
			}
			lh.progressChan <- &ListProgress{
				Files:   int(atomic.LoadInt32(&lh.filecount)),
				Folders: int(atomic.LoadInt32(&lh.foldcount)), Done: false}
			progTimer = time.Now()
		}

	}
	lh.local.store.Save("folders.json", "files.json", "idmap.json")
	lh.progressChan <- &ListProgress{Files: int(lh.filecount), Folders: int(lh.foldcount),
		Done: true}

}

func (lh *ListHdl) recursiveFoldsearch(args interface{}) {
	unpackArgs := args.([1]interface{})
	folderRel, ok := unpackArgs[0].(string)
	if !ok {
		atomic.AddInt32(&lh.onGoingRequests, -1)
		return
	}
	folderAbs := filepath.Join(lh.local.rootDir, folderRel)
	folders, err := ioutil.ReadDir(folderAbs)
	checkErr(err)

	for _, fol := range folders {
		relpath := filepath.Join(folderRel, fol.Name())
		if fol.IsDir() {

			lh.folderQueue.Enqueue(relpath)
			aa := new(FoldHolder)
			aa.ModTime = fol.ModTime().UTC().Format(time.RFC3339)
			aa.Dir = folderRel
			aa.Name = fol.Name()
			lh.storeW.WriteFold(relpath, aa, true)
			atomic.AddInt32(&lh.foldcount, 1)
		} else {

			aa := new(FileHolder)
			aa.ModTime = fol.ModTime().UTC().Format(time.RFC3339)
			aa.Dir = folderRel
			aa.Name = fol.Name()
			aa.Md5Chk = lh.local.hashsum(relpath)
			mime, err := mimetype.DetectFile(filepath.Join(folderAbs, fol.Name()))
			checkErr(err)
			aa.MimeType = mime.String()
			lh.storeW.WriteFile(relpath, aa, true)
			atomic.AddInt32(&lh.filecount, 1)
		}
	}

	atomic.AddInt32(&lh.onGoingRequests, -1)
}

// Progress returns the progress channel
func (lh *ListHdl) Progress() <-chan *ListProgress {
	return lh.progressChan
}

// Error returns the error channel
func (lh *ListHdl) Error() <-chan error {
	return lh.errChan
}

// SendComd sends command to
func (lh *ListHdl) SendComd() {
	lh.local.canRunList = false
}

func (lh *ListHdl) onListError() {
	if err := recover(); err != nil {
		err1 := err.(error)
		lh.errChan <- err1
	}
}
