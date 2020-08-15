package gdrive

import (
	"errors"
	"fmt"
	"github.com/oleiade/lane"
	"github.com/panjf2000/ants/v2"
	"godrive/internal/googleclient"
	googledrive "google.golang.org/api/drive/v3"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxGoroutine    = 15
	minGoroutine    = 2
	batchSize       = 100
	minWaitingBatch = 4

	// C_CANCEL cancels the listAll operation
	C_CANCEL int8 = 1
	// S_ACK state command ACK
	S_ACK int8 = -1
	// S_TERM state terminated
	S_TERM int8 = -2
)

var (
	// ErrCancel is the error thrown by the canceled operation
	ErrCancel = errors.New("The operation is canceled")
)

// DriveClient represents a google drive client object
type DriveClient struct {
	service             *googledrive.Service
	canRunList          bool
	isListRunning       bool
	localRoot           string
	remoteRootID        string
	store               *GDStore
	userRateLimitExceed *regexp.Regexp
}

type foldBatch struct {
	ids           []string
	nextPageToken string
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func makeBatch(ids []string, nextPage string) *foldBatch {
	root := new(foldBatch)
	root.ids = ids
	root.nextPageToken = nextPage
	return root
}

// NewClient a new googledrive client (localDirPath, remoteRootID)
func NewClient(localDir string, remoteID string, store *GDStore) (*DriveClient, error) {
	client := new(DriveClient)
	var err error
	client.service, err = googleclient.NewService(0)
	if err != nil {
		return nil, err
	}

	client.isListRunning = false
	client.localRoot = localDir
	client.remoteRootID = remoteID
	client.store = store
	client.userRateLimitExceed = regexp.MustCompile("User Rate Limit Exceeded")
	return client, nil
}

func (ls *listStruct) onListError() {
	if err := recover(); err != nil {
		err1 := err.(error)
		ls.errChan <- errors.New("DriveClient list error: " + err1.Error() +
			"\n" + string(debug.Stack()))
	}
}

// ListProgress of the command
type ListProgress struct {
	Files   int
	Folders int
	Done    bool
}

// ListAll write a list of folders and files to "location". Returns ListProgress struct if not already running, else returns nil
func (drive *DriveClient) ListAll() *ListHdl {
	if drive.isListRunning {
		return nil
	}
	progressChan := make(chan *ListProgress, 10)
	commandChan := make(chan int8)
	errChan := make(chan error)
	ss := new(listStruct)
	ss.progressChan = progressChan
	ss.commandChan = commandChan
	ss.errChan = errChan
	ss.drive = drive
	result := new(ListHdl)
	result.progressChan = progressChan
	result.commandChan = commandChan
	result.errChan = errChan
	result.running = false
	go ss.listAll()
	return result
}

// ListHdl The handle to ListAll
type ListHdl struct {
	progressChan <-chan *ListProgress
	errChan      <-chan error
	commandChan  chan int8
	running      bool
}

// Cancel sends command to ListAll. Returns ErrCancel if the operation is already canceled
func (hd *ListHdl) Cancel() error {
	select {
	case hd.commandChan <- C_CANCEL:

		a := <-hd.commandChan
		switch a {
		case S_TERM:
			hd.commandChan <- S_TERM
		case S_ACK:
		}

		return nil
	default:
		return ErrCancel
	}
}

// Progress returns the channel for progress
func (hd *ListHdl) Progress() <-chan *ListProgress {
	return hd.progressChan
}

func (hd *ListHdl) Error() <-chan error {
	return hd.errChan
}

type listStruct struct {
	drive              *DriveClient
	storeID            [3]*AccessLock
	progressChan       chan *ListProgress
	commandChan        chan int8
	errChan            chan error
	foldersearchQueue  *lane.Queue
	folderUnbatchSlice []string
	unBatchMux         sync.Mutex
	onGoingRequests    int32
	requestInterv      int32
	filecount          int32
	foldcount          int32
}

func (ls *listStruct) listAll() {
	drive := ls.drive
	p, errP := ants.NewPoolWithFunc(maxGoroutine, ls.recursiveFoldSearch)
	if errP != nil {
		log.Fatalf("There is a problem starting goroutine pool: %v", errP)
	}

	defer p.Release()
	defer ls.onListError()
	var err1, err2, err3 error
	ls.storeID[0], err1 = ls.drive.store.Acquire(R_FILEMAP)
	ls.storeID[1], err2 = ls.drive.store.Acquire(R_FOLDMAP)
	ls.storeID[2], err3 = ls.drive.store.Acquire(R_PATHMAP)
	checkErr(err1)
	checkErr(err2)
	checkErr(err3)
	defer func() {
		err1 = ls.drive.store.Release(ls.storeID[0])
		err2 = ls.drive.store.Release(ls.storeID[1])
		err3 = ls.drive.store.Release(ls.storeID[2])
		checkErr(err1)
		checkErr(err2)
		checkErr(err3)
		drive.isListRunning = false
	}()
	drive.canRunList = true
	drive.isListRunning = true
	ls.onGoingRequests = 0
	ls.requestInterv = 20

	ls.onGoingRequests = 0
	ls.foldersearchQueue = lane.NewQueue()
	ls.folderUnbatchSlice = make([]string, 0, batchSize)
	ll := make([]string, 0, 1)
	ll = append(ll, drive.remoteRootID)

	ls.foldersearchQueue.Enqueue(makeBatch(ll, ""))

	ls.filecount, ls.foldcount = 0, 0
	var workDone bool = false
	go ls.getComd()
	progTimer := time.Now()

	for !workDone && drive.canRunList {

		largeQueue := ls.foldersearchQueue.Size() > minWaitingBatch

		if largeQueue {
			for i := 0; i < maxGoroutine; i++ {
				atomic.AddInt32(&ls.onGoingRequests, 1)
				ls.unBatchMux.Lock()
				if len(ls.folderUnbatchSlice) >= batchSize {

					ls.foldersearchQueue.Enqueue(
						makeBatch(ls.folderUnbatchSlice[:batchSize], ""))
					ls.folderUnbatchSlice = ls.folderUnbatchSlice[batchSize:]
				}
				ls.unBatchMux.Unlock()

				checkErr(p.Invoke([1]interface{}{
					ls.foldersearchQueue.Dequeue()}))
			}

		} else if !largeQueue && maxGoroutine-p.Free() <= minGoroutine {
			atomic.AddInt32(&ls.onGoingRequests, 1)
			ls.unBatchMux.Lock()
			if len(ls.folderUnbatchSlice) >= batchSize {

				ls.foldersearchQueue.Enqueue(
					makeBatch(ls.folderUnbatchSlice[:batchSize], ""))
				ls.folderUnbatchSlice = ls.folderUnbatchSlice[batchSize:]
			} else if len(ls.folderUnbatchSlice) > 0 {
				ls.foldersearchQueue.Enqueue(makeBatch(ls.folderUnbatchSlice, ""))
				ls.folderUnbatchSlice = make([]string, 0, batchSize)
			}
			ls.unBatchMux.Unlock()
			checkErr(p.Invoke([1]interface{}{
				ls.foldersearchQueue.Dequeue()}))
			time.Sleep(100 * time.Millisecond) // sleep longer
		}
		if atomic.LoadInt32(&ls.requestInterv) > 0 {
			atomic.AddInt32(&ls.requestInterv, -10)
		}
		time.Sleep(time.Duration(atomic.LoadInt32(&ls.requestInterv)) *
			time.Millisecond) // preventing exceed user rate limit

		if time.Now().Sub(progTimer).Milliseconds() >= 1000 {
			if len(ls.progressChan) >= 9 {
				<-ls.progressChan
			}
			ls.progressChan <- &ListProgress{Files: int(atomic.LoadInt32(&ls.filecount)),
				Folders: int(atomic.LoadInt32(&ls.foldcount)), Done: false}
			progTimer = time.Now()
		}

		ls.unBatchMux.Lock()
		workDone = ls.foldersearchQueue.Empty() &&
			atomic.LoadInt32(&ls.onGoingRequests) == 0 &&
			len(ls.folderUnbatchSlice) == 0
		ls.unBatchMux.Unlock()

	}

	ls.progressChan <- &ListProgress{Files: int(ls.filecount),
		Folders: int(ls.foldcount), Done: drive.canRunList}

}

func (ls *listStruct) recursiveFoldSearch(args interface{}) {
	drive := ls.drive
	unpackArgs := args.([1]interface{})

	batch, ok := unpackArgs[0].(*foldBatch)
	if !ok || len(batch.ids) == 0 {
		atomic.AddInt32(&ls.onGoingRequests, -1)
		return
	}
	defer ls.onListError()
	// fmt.Printf("recursiveFold\n")

	var str strings.Builder
	str.WriteString("(")
	for index, a := range batch.ids {
		str.WriteString("'")
		str.WriteString(a)
		str.WriteString("' in parents")
		if index < len(batch.ids)-1 {
			str.WriteString(" or ")
		}
	}
	str.WriteString(") and trashed=false")
	// fmt.Printf("string buffer: %s\n", str.String())

	r, err := drive.service.Files.List().PageSize(1000).
		Fields("nextPageToken, files(id, name, mimeType, modifiedTime, md5Checksum, parents)").
		Q(str.String()).PageToken(batch.nextPageToken).
		Spaces("drive").Corpora("user").Do()
	if err != nil {

		match := drive.userRateLimitExceed.FindString(err.Error())
		if match != "" {
			ls.foldersearchQueue.Enqueue(batch)
			atomic.AddInt32(&ls.requestInterv, 200)
			atomic.AddInt32(&ls.onGoingRequests, -1)
			fmt.Printf("rate limit: %v\n", err)
			return
		}
		checkErr(err)

	}

	if r.NextPageToken != "" {
		batch.nextPageToken = r.NextPageToken
		ls.foldersearchQueue.Enqueue(batch)
	}
	ll := make([]string, 0, batchSize)

	var parentPath string
	if len(r.Files) > 0 {
		fol, err := ls.storeID[1].AccessFold(r.Files[0].Parents[0], true)
		if err == ErrNotFound {
			parentPath = "/"
			ls.storeID[2].AccessIDMap("/", true, &r.Files[0].Parents[0])

		} else {
			parentPath = filepath.Join(fol.Dir, fol.Name)
		}

	}

	for _, file := range r.Files {

		if file.MimeType == "application/vnd.google-apps.folder" {
			ll = append(ll, file.Id)
			fold, _ := ls.storeID[1].AccessFold(file.Id, false)
			*fold = *convFolStruct(file, parentPath)
			err := ls.storeID[2].AccessIDMap(filepath.Join(parentPath,
				file.Name), true, &file.Id)
			_ = err
			atomic.AddInt32(&ls.foldcount, 1)
		} else {

			ff, _ := ls.storeID[0].AccessFile(file.Id, false)
			*ff = *convFilStruct(file, parentPath)
			atomic.AddInt32(&ls.filecount, 1)
		}

		if len(ll) >= batchSize {
			ls.foldersearchQueue.Enqueue(makeBatch(ll, ""))
			ll = make([]string, 0, batchSize)
		}
	}
	if len(ll) > 0 {
		ls.unBatchMux.Lock()
		ls.folderUnbatchSlice = append(ls.folderUnbatchSlice, ll...)
		ls.unBatchMux.Unlock()
		// drive.foldersearchQueue.Enqueue(makeBatch(ll, ""))
	}

	atomic.AddInt32(&ls.onGoingRequests, -1)

}

// FileHolder holds a file
type FileHolder struct {
	Name     string
	MimeType string
	ModTime  string
	Parents  []string
	Md5Chk   string
	Dir      string
}

// FoldHolder holds a folder
type FoldHolder struct {
	Name     string
	MimeType string
	ModTime  string
	Parents  []string
	Dir      string
}

func convFolStruct(file *googledrive.File, path string) *FoldHolder {
	aa := new(FoldHolder)
	aa.Name = file.Name
	aa.MimeType = file.MimeType
	aa.ModTime = file.ModifiedTime
	aa.Parents = file.Parents
	aa.Dir = path
	return aa
}

func convFilStruct(file *googledrive.File, path string) *FileHolder {
	aa := new(FileHolder)
	aa.Name = file.Name
	aa.MimeType = file.MimeType
	aa.ModTime = file.ModifiedTime
	aa.Parents = file.Parents
	aa.Md5Chk = file.Md5Checksum
	aa.Dir = path
	return aa
}

func (ls *listStruct) getComd() {
	for ls.drive.canRunList {

		b := <-ls.commandChan
		switch b {
		case C_CANCEL:
			ls.drive.canRunList = false
			ls.commandChan <- S_TERM
		default:
			ls.commandChan <- S_ACK // ACK
		}

	}

}

// UDLProgress represents the download progress
type UDLProgress struct {
	Percentage float32
	Done       bool
	Err        error
	File       *googledrive.File
}

type writeCounter struct {
	totalBytes int64
	accu       int64
	prog       chan *UDLProgress
	now        time.Time
}

func (wc *writeCounter) Write(p []byte) (int, error) {
	n := len(p)
	wc.accu += int64(n)

	if time.Now().Sub(wc.now).Seconds() > 1 {
		if wc.totalBytes == 0 {
			wc.prog <- &UDLProgress{Percentage: -1, Done: false, Err: nil, File: nil}
		} else {
			wc.prog <- &UDLProgress{Percentage: float32(wc.accu) / float32(wc.totalBytes) * 100,
				Done: false, Err: nil, File: nil}
		}
		wc.now = time.Now()
	}

	return n, nil

}

func onDLError(ch chan *UDLProgress) {
	if err := recover(); err != nil {
		err1 := err.(error)
		var pr *UDLProgress = &UDLProgress{Done: false, Percentage: -1, Err: err1, File: nil}
		ch <- pr
	}
}

func onULError(ch chan *UDLProgress) {
	if err := recover(); err != nil {
		err1 := err.(error)
		var pr *UDLProgress = &UDLProgress{Done: false, Percentage: -1, Err: err1, File: nil}
		ch <- pr
	}
}

// Download a file
func (drive *DriveClient) Download(fileID string, dest string) chan *UDLProgress {
	ch := make(chan *UDLProgress, 10)
	go drive.download(fileID, dest, ch)
	return ch
}

func (drive *DriveClient) download(fileID string, dest string, ch chan *UDLProgress) {
	defer close(ch)
	defer onDLError(ch)
	info, errI := drive.service.Files.Get(fileID).Fields("name, size, mimeType").Do()
	checkErr(errI)
	_ = info
	res, errD := drive.service.Files.Get(fileID).AcknowledgeAbuse(false).Download()
	checkErr(errD)
	defer res.Body.Close()
	path := filepath.Join(drive.localRoot, dest, info.Name)
	file, filerr := os.Create(path)
	checkErr(filerr)
	defer file.Close()
	wc := &writeCounter{totalBytes: int64(info.Size), accu: 0, prog: ch}
	_, errC := io.Copy(file, io.TeeReader(res.Body, wc))
	if errC != ErrCancel {
		checkErr(errC)
	}

	ch <- &UDLProgress{Percentage: 100, Done: true, Err: nil, File: info}

}

// Upload a file
func (drive *DriveClient) Upload(metadata *googledrive.File, path string) chan *UDLProgress {
	ch := make(chan *UDLProgress, 10)
	go drive.upload(metadata, path, ch)
	return ch
}

func (drive *DriveClient) upload(metadata *googledrive.File, path string, ch chan *UDLProgress) {
	defer close(ch)
	defer onULError(ch)
	content, errOpen := os.Open(filepath.Join(drive.localRoot, path))
	checkErr(errOpen)
	stat, errStat := content.Stat()
	checkErr(errStat)
	wc := &writeCounter{totalBytes: stat.Size(), accu: 0, prog: ch}
	file, err := drive.service.Files.Create(metadata).EnforceSingleParent(true).
		Media(io.TeeReader(content, wc)).Do()
	if err != ErrCancel {
		checkErr(err)
	}

	ch <- &UDLProgress{Percentage: 100, Done: true, Err: nil, File: file}
}

// MkDirProgress represents the mkdir progress
type MkDirProgress struct {
	Done bool
	Err  error
	File *googledrive.File
}

func onMkdirError(ch chan *MkDirProgress) {
	if err := recover(); err != nil {
		err1 := err.(error)
		var pr *MkDirProgress = &MkDirProgress{Done: false, Err: err1, File: nil}
		ch <- pr
	}
}

// MkdirAll mkdir recursively
func (drive *DriveClient) MkdirAll(path string) chan *MkDirProgress {
	var ch chan *MkDirProgress = make(chan *MkDirProgress, 10)
	go drive.mkdirall(path, ch)
	return ch
}

func (drive *DriveClient) mkdirall(path string, ch chan *MkDirProgress) {
	defer close(ch)
	defer onMkdirError(ch)

	var par []string
	ss := strings.Split(filepath.Dir(path), "/")[1:]
	if len(ss) > 1 || (len(ss) == 1 && ss[0] != "") {
		par = ss
	}
	dirname := filepath.Base(path)
	d := &googledrive.File{
		Name:     dirname,
		MimeType: "application/vnd.google-apps.folder",
		Parents:  []string{"parentId"},
	}

	file, err := drive.service.Files.Create(d).Do()
	checkErr(err)
	ch <- &MkDirProgress{Done: true, Err: nil, File: file}
	_, _ = par, d
}
