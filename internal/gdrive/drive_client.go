package gdrive

import (
	"errors"
	"fmt"
	"github.com/oleiade/lane"
	"github.com/panjf2000/ants/v2"
	"godrive/internal/googleclient"
	"godrive/internal/settings"
	"godrive/internal/utils"
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
	maxGoroutine    = 20
	minGoroutine    = 2
	batchSize       = 100
	minWaitingBatch = 4
	listAllQuery    = "nextPageToken, files(id, name, mimeType," +
		" modifiedTime, md5Checksum, parents)"

	// C_CANCEL cancels the listAll operation
	C_CANCEL int8 = 1
	// C_QUERY queries the state
	C_QUERY int8 = 2

	// S_ACK state ACK received
	S_ACK int8 = -1
	// S_TERM state terminated
	S_TERM int8 = -2
	// S_RUNNING state running
	S_RUNNING int8 = -3
)

var (
	// ErrJammed is the error thrown when the channel is jammed
	ErrJammed = errors.New("The channel is jammed")
	// ErrCanceled is the error thrown when the operation is canceled
	ErrCanceled = errors.New("The operation is canceled")
	// ErrNoResponse is the error thrown when the command does return
	// ACK before timeout
	ErrNoResponse = errors.New("There is no response from the receiver")
)

// DriveClient represents a google drive client object
type DriveClient struct {
	service             *googledrive.Service
	canRunList          bool
	isListRunning       bool
	localRoot           string
	store               *GDStore
	userRateLimitExceed *regexp.Regexp
	userID              string
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
func NewClient(id string) (*DriveClient, error) {
	client := new(DriveClient)
	var err error
	client.service, err = googleclient.NewService(id)
	if err != nil {
		return nil, err
	}
	set, err := settings.ReadDriveConfig()
	if err != nil {
		return nil, err
	}
	local, err := set.GetUser(id)
	if err != nil {
		return nil, err
	}
	client.localRoot = local.LocalRoot

	store, err := NewStore(id)
	if err != nil {
		return nil, err
	}
	client.store = store
	client.userID = id
	client.isListRunning = false
	client.userRateLimitExceed = regexp.MustCompile("User Rate Limit Exceeded")
	return client, nil
}

func (lh *ListHdl) onListError() {
	if err := recover(); err != nil {
		err1 := err.(error)
		err1 = fmt.Errorf("%w\n%s", err1, string(debug.Stack()))
		lh.errChan <- fmt.Errorf("DriveClient list error\n%w", err1)
	}
}

// ListProgress of the command
type ListProgress struct {
	Files   int
	Folders int
	Done    bool
}

// ListHdl The handle to ListAll
type ListHdl struct {
	progressChan       chan *ListProgress
	errChan            chan error
	commandChan        chan int8
	drive              *DriveClient
	storeW             StoreWrite
	foldersearchQueue  *lane.Queue
	folderUnbatchSlice []string
	unBatchMux         sync.Mutex
	onGoingRequests    int32
	requestInterv      int32
	filecount          int32
	foldcount          int32
}

// SendComd sends command to ListAll. Returns error
func (lh *ListHdl) SendComd(command int8) error {
	start := time.Now()
	for time.Now().Sub(start).Milliseconds() <= 500 { // timeout is 500ms
		select {
		case lh.commandChan <- command:

			a := <-lh.commandChan
			switch a {
			case S_TERM:
				lh.commandChan <- S_TERM
			case S_ACK:
			}

			return nil
		default:
			time.Sleep(20 * time.Millisecond)
		}
	}

	return ErrJammed

}

// Progress returns the channel for progress
func (lh *ListHdl) Progress() <-chan *ListProgress {
	return lh.progressChan
}

func (lh *ListHdl) Error() <-chan error {
	return lh.errChan
}

// ListAll write a list of folders and files to "location".
// Returns ListProgress struct if not already running, else returns nil
func (drive *DriveClient) ListAll() *ListHdl {
	if drive.isListRunning {
		return nil
	}
	progressChan := make(chan *ListProgress, 5)
	commandChan := make(chan int8)
	errChan := make(chan error, 5)

	result := new(ListHdl)
	result.progressChan = progressChan
	result.commandChan = commandChan
	result.errChan = errChan
	result.drive = drive
	go result.listAll()
	return result
}

func (lh *ListHdl) listAll() {

	p, errP := ants.NewPoolWithFunc(maxGoroutine, lh.recursiveFoldSearch)
	if errP != nil {
		log.Fatalf("There is a problem starting goroutine pool: %v", errP)
	}

	defer p.Release()
	defer lh.onListError()

	var err error

	lh.storeW, err = lh.drive.store.AcquireWrite(true)
	checkErr(err)
	defer func() {
		err = lh.storeW.Release()
		checkErr(err)
		lh.drive.isListRunning = false
		lh.drive.canRunList = false
	}()
	lh.drive.canRunList = true
	lh.drive.isListRunning = true
	lh.onGoingRequests = 0
	lh.requestInterv = 20

	lh.onGoingRequests = 0
	lh.foldersearchQueue = lane.NewQueue()
	lh.folderUnbatchSlice = make([]string, 0, batchSize)
	ll := make([]string, 0, 1)
	ll = append(ll, "root")

	lh.foldersearchQueue.Enqueue(makeBatch(ll, ""))

	lh.filecount, lh.foldcount = 0, 0
	var workDone bool = false
	go lh.getComd()
	progTimer := time.Now()

	for !workDone && lh.drive.canRunList {

		largeQueue := lh.foldersearchQueue.Size() > minWaitingBatch

		if largeQueue {
			for i := 0; i < p.Free(); i++ {
				atomic.AddInt32(&lh.onGoingRequests, 1)
				lh.unBatchMux.Lock()
				if len(lh.folderUnbatchSlice) >= batchSize {

					lh.foldersearchQueue.Enqueue(
						makeBatch(lh.folderUnbatchSlice[:batchSize], ""))
					lh.folderUnbatchSlice = lh.folderUnbatchSlice[batchSize:]
				}
				lh.unBatchMux.Unlock()

				checkErr(p.Invoke([1]interface{}{
					lh.foldersearchQueue.Dequeue()}))
			}

		} else if !largeQueue && maxGoroutine-p.Free() <= minGoroutine {
			atomic.AddInt32(&lh.onGoingRequests, 1)
			lh.unBatchMux.Lock()
			if len(lh.folderUnbatchSlice) >= batchSize {

				lh.foldersearchQueue.Enqueue(
					makeBatch(lh.folderUnbatchSlice[:batchSize], ""))
				lh.folderUnbatchSlice = lh.folderUnbatchSlice[batchSize:]
			} else if len(lh.folderUnbatchSlice) > 0 {
				lh.foldersearchQueue.Enqueue(makeBatch(lh.folderUnbatchSlice, ""))
				lh.folderUnbatchSlice = make([]string, 0, batchSize)
			}
			lh.unBatchMux.Unlock()
			checkErr(p.Invoke([1]interface{}{
				lh.foldersearchQueue.Dequeue()}))
			time.Sleep(100 * time.Millisecond) // sleep longer
		}
		if atomic.LoadInt32(&lh.requestInterv) > 0 {
			atomic.AddInt32(&lh.requestInterv, -10)
		}
		time.Sleep(time.Duration(atomic.LoadInt32(&lh.requestInterv)) *
			time.Millisecond) // preventing exceed user rate limit

		if time.Now().Sub(progTimer).Milliseconds() >= 1000 {
			if len(lh.progressChan) >= 4 {
				<-lh.progressChan
			}
			lh.progressChan <- &ListProgress{
				Files:   int(atomic.LoadInt32(&lh.filecount)),
				Folders: int(atomic.LoadInt32(&lh.foldcount)), Done: false}
			progTimer = time.Now()
		}

		lh.unBatchMux.Lock()
		workDone = lh.foldersearchQueue.Empty() &&
			atomic.LoadInt32(&lh.onGoingRequests) == 0 &&
			len(lh.folderUnbatchSlice) == 0
		lh.unBatchMux.Unlock()

	}

	lh.progressChan <- &ListProgress{Files: int(lh.filecount),
		Folders: int(lh.foldcount), Done: lh.drive.canRunList}

}

func (lh *ListHdl) recursiveFoldSearch(args interface{}) {
	drive := lh.drive
	unpackArgs := args.([1]interface{})

	batch, ok := unpackArgs[0].(*foldBatch)
	if !ok || len(batch.ids) == 0 {
		atomic.AddInt32(&lh.onGoingRequests, -1)
		return
	}
	defer lh.onListError()

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

	r, err := drive.service.Files.List().PageSize(1000).
		Fields(listAllQuery).
		Q(str.String()).PageToken(batch.nextPageToken).
		Spaces("drive").Corpora("user").Do()
	if err != nil {

		match := drive.userRateLimitExceed.FindString(err.Error())
		if match != "" {
			lh.foldersearchQueue.Enqueue(batch)
			atomic.AddInt32(&lh.requestInterv, 200)
			atomic.AddInt32(&lh.onGoingRequests, -1)
			fmt.Printf("rate limit: %v\n", err)
			return
		}
		checkErr(err)

	}

	if r.NextPageToken != "" {
		batch.nextPageToken = r.NextPageToken
		lh.foldersearchQueue.Enqueue(batch)
	}
	ll := make([]string, 0, batchSize)

	var parentPath string
	if len(r.Files) > 0 {
		fol, err := lh.storeW.ReadFold(r.Files[0].Parents[0], true)
		if errors.Is(err, ErrNotFound) {
			parentPath = "/"
			lh.storeW.WriteIDMap("/", r.Files[0].Parents[0], true)

		} else {
			parentPath = filepath.Join(fol.Dir, fol.Name)
		}

	}

	for _, file := range r.Files {

		if file.MimeType == "application/vnd.google-apps.folder" {
			ll = append(ll, file.Id)

			lh.storeW.WriteFold(file.Id, convFolStruct(file, parentPath), true)
			err := lh.storeW.WriteIDMap(filepath.Join(parentPath,
				file.Name), file.Id, true)
			_ = err
			atomic.AddInt32(&lh.foldcount, 1)
		} else {

			lh.storeW.WriteFile(file.Id, convFilStruct(file, parentPath), false)
			atomic.AddInt32(&lh.filecount, 1)
		}

		if len(ll) >= batchSize {
			lh.foldersearchQueue.Enqueue(makeBatch(ll, ""))
			ll = make([]string, 0, batchSize)
		}
	}
	if len(ll) > 0 {
		lh.unBatchMux.Lock()
		lh.folderUnbatchSlice = append(lh.folderUnbatchSlice, ll...)
		lh.unBatchMux.Unlock()
		// drive.foldersearchQueue.Enqueue(makeBatch(ll, ""))
	}

	atomic.AddInt32(&lh.onGoingRequests, -1)

}

func convFolStruct(file *googledrive.File, path string) *FoldHolder {
	aa := new(FoldHolder)
	aa.Name = file.Name
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

func (lh *ListHdl) getComd() {
	for lh.drive.isListRunning {

		b := <-lh.commandChan
		switch b {
		case C_CANCEL:
			lh.drive.canRunList = false
			lh.commandChan <- S_TERM
		default:
			lh.commandChan <- b
			time.Sleep(100 * time.Millisecond) // prevent further reading
		}

	}

}

// UDLProgress represents the download progress
type UDLProgress struct {
	Percentage float32
	Done       bool
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
			wc.prog <- &UDLProgress{Percentage: -1, Done: false, File: nil}
		} else {
			wc.prog <- &UDLProgress{
				Percentage: float32(wc.accu) / float32(wc.totalBytes) * 100,
				Done:       false,
				File:       nil}
		}
		wc.now = time.Now()
	}

	return n, nil

}

func (dl *DownloadHdl) onDLError() {
	if err := recover(); err != nil {
		err1 := err.(error)
		dl.errChan <- utils.NewError(errors.New("DriveClient download error"), err1)

	}
}

func (ul *UploadHdl) onULError() {
	if err := recover(); err != nil {
		err1 := err.(error)
		ul.errChan <- utils.NewError(errors.New("DriveClient upload error"), err1)
	}
}

// DownloadHdl is the handle returned by Download
type DownloadHdl struct {
	downloadProgChan chan *UDLProgress
	errChan          chan error
	commandChan      chan int8
	fileID           string
	dest             string
	drive            *DriveClient
}

// Download a file
func (drive *DriveClient) Download(fileID string, dest string) *DownloadHdl {
	ch := make(chan *UDLProgress, 5)
	errChan := make(chan error, 5)
	comd := make(chan int8)
	dd := new(DownloadHdl)
	dd.downloadProgChan = ch
	dd.errChan = errChan
	dd.commandChan = comd
	dd.fileID = fileID
	dd.dest = dest
	dd.drive = drive
	go dd.download()
	return dd
}

func (dl *DownloadHdl) download() {

	defer dl.onDLError()
	info, errI := dl.drive.service.Files.Get(dl.fileID).
		Fields("name, size, mimeType").Do()
	checkErr(errI)
	_ = info
	res, errD := dl.drive.service.Files.Get(dl.fileID).
		AcknowledgeAbuse(false).Download()
	checkErr(errD)
	defer res.Body.Close()
	path := filepath.Join(dl.drive.localRoot, dl.dest, info.Name)
	file, filerr := os.Create(path)
	checkErr(filerr)
	defer file.Close()
	wc := &writeCounter{
		totalBytes: int64(info.Size),
		accu:       0,
		prog:       dl.downloadProgChan}
	_, errC := io.Copy(file, io.TeeReader(res.Body, wc))
	if !errors.Is(errC, ErrCanceled) {
		checkErr(errC)
	}

	dl.downloadProgChan <- &UDLProgress{Percentage: 100, Done: true, File: info}

}

// UploadHdl is the handle returned by Upload
type UploadHdl struct {
	uploadProgChan chan *UDLProgress
	errChan        chan error
	commandChan    chan int8
	metadata       *googledrive.File
	path           string
	drive          *DriveClient
}

// Upload a file
func (drive *DriveClient) Upload(metadata *googledrive.File, path string) *UploadHdl {
	ch := make(chan *UDLProgress, 5)
	errChan := make(chan error, 5)
	comd := make(chan int8)
	ul := new(UploadHdl)
	ul.uploadProgChan = ch
	ul.errChan = errChan
	ul.commandChan = comd
	ul.drive = drive
	ul.path = path
	ul.metadata = metadata
	go ul.upload()
	return ul
}

func (ul *UploadHdl) upload() {

	defer ul.onULError()
	content, errOpen := os.Open(filepath.Join(ul.drive.localRoot, ul.path))
	checkErr(errOpen)
	stat, errStat := content.Stat()
	checkErr(errStat)
	wc := &writeCounter{totalBytes: stat.Size(),
		accu: 0,
		prog: ul.uploadProgChan}
	file, err := ul.drive.service.Files.Create(ul.metadata).
		EnforceSingleParent(true).Media(io.TeeReader(content, wc)).Do()
	if !errors.Is(err, ErrCanceled) {
		checkErr(err)
	}

	ul.uploadProgChan <- &UDLProgress{Percentage: 100, Done: true, File: file}
}

// MkDirProgress represents the mkdir progress
type MkDirProgress struct {
	Done bool
	File *googledrive.File
}

func onMkdirError(ch chan *MkDirProgress) {
	if err := recover(); err != nil {
		err1 := err.(error)
		_ = err1
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
	ch <- &MkDirProgress{Done: true, File: file}
	_, _ = par, d
}
