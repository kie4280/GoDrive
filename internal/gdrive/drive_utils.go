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
	"os"
	"path"
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
	// S_RUNNING state running
	S_RUNNING int8 = -2
)

var (

	// ErrTerminated is the error thrown when the operation is canceled
	ErrTerminated = errors.New("The operation is already terminated")
	// ErrNoResponse is the error thrown when the command does return
	// ACK before timeout
	ErrNoResponse = errors.New("There is no response from the receiver")
	// ErrJammed the command channel is jammed
	ErrJammed = errors.New("The command channel is jammed")
	// ErrDuplicated is the error thrown when there are files that have
	// the same name in a directory
	ErrDuplicated = errors.New("Duplicated files")
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
	user                settings.UserConfig
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
	client.user, err = set.GetUser(id)
	if err != nil {
		return nil, err
	}
	client.localRoot = client.user.GetLocalRoot()

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
		err1 = utils.NewError(err1, errors.New(string(debug.Stack())))
		lh.errChan <- utils.NewError(errors.New("DriveClient list error"), err1)
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
	replyChan          chan int8
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
	if !lh.drive.isListRunning {
		return ErrTerminated
	}
	start := time.Now()
	for time.Now().Sub(start).Milliseconds() <= 500 { // timeout is 500ms
		select {
		case lh.commandChan <- command:
			return nil
		default:
			time.Sleep(20 * time.Millisecond)
		}
	}
	start = time.Now()
	for time.Now().Sub(start).Milliseconds() <= 500 { // timeout is 500ms
		select {
		case response := <-lh.replyChan:

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
	result := new(ListHdl)
	result.progressChan = make(chan *ListProgress, 5)
	result.errChan = make(chan error, 5)
	result.commandChan = make(chan int8)
	result.replyChan = make(chan int8)
	result.drive = drive
	go result.listAll()
	return result
}

func (lh *ListHdl) listAll() {
	defer lh.onListError()
	lh.drive.isListRunning = true
	p, errP := ants.NewPoolWithFunc(maxGoroutine, lh.recursiveFoldSearch)
	checkErr(errP)
	defer p.Release()

	var err error
	lh.storeW, err = lh.drive.store.AcquireWrite(true)
	checkErr(err)
	defer func() {
		err := lh.storeW.Release()
		checkErr(err)
		lh.drive.isListRunning = false
		lh.drive.canRunList = false
	}()
	lh.drive.canRunList = true
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
	lh.drive.store.Save("folders.json", "files.json", "foldIDMaps.json")
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
			parentPath = path.Join(fol.Dir, fol.Name)
		}

	}

	for _, file := range r.Files {

		if file.MimeType == "application/vnd.google-apps.folder" {
			ll = append(ll, file.Id)

			lh.storeW.WriteFold(file.Id, convFolStruct(file, parentPath), true)
			err := lh.storeW.WriteIDMap(path.Join(parentPath,
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
			lh.replyChan <- S_ACK
		case C_QUERY:
			lh.replyChan <- S_ACK
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

// SendComd sends commands to Download worker
func (dl *DownloadHdl) SendComd(command int8) error {
	if !dl.isRunning {
		return ErrTerminated
	}
	var sent bool = false
	start := time.Now()
	for time.Now().Sub(start).Milliseconds() <= 500 { // timeout is 500ms
		select {
		case dl.commandChan <- command:
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
		case response := <-dl.replyChan:

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

func (ul *UploadHdl) onULError() {
	if err := recover(); err != nil {
		err1 := err.(error)
		ul.errChan <- utils.NewError(errors.New("DriveClient upload error"), err1)
	}
}

// SendComd sends commands to upload worker
func (ul *UploadHdl) SendComd(command int8) error {
	if !ul.isRunning {
		return ErrTerminated
	}
	var sent bool = false
	start := time.Now()
	for time.Now().Sub(start).Milliseconds() <= 500 { // timeout is 500ms
		select {
		case ul.commandChan <- command:
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
		case response := <-ul.replyChan:

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

// DownloadHdl is the handle returned by Download
type DownloadHdl struct {
	downloadProgChan chan *UDLProgress
	errChan          chan error
	commandChan      chan int8
	replyChan        chan int8
	fileID           string
	dest             string
	drive            *DriveClient
	isRunning        bool
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
	dd.isRunning = true
	go dd.download()
	return dd
}

func (dl *DownloadHdl) download() {
	defer dl.onDLError()
	defer func() {
		dl.isRunning = false
	}()
	info, errI := dl.drive.service.Files.Get(dl.fileID).
		Fields("name, size, mimeType").Do()
	checkErr(errI)
	_ = info
	res, errD := dl.drive.service.Files.Get(dl.fileID).
		AcknowledgeAbuse(false).Download()
	checkErr(errD)
	defer res.Body.Close()
	filePath := path.Join(dl.drive.localRoot, dl.dest, info.Name)
	file, filerr := os.Create(filePath)
	checkErr(filerr)
	defer file.Close()
	wc := &writeCounter{
		totalBytes: int64(info.Size),
		accu:       0,
		prog:       dl.downloadProgChan}
	_, errC := io.Copy(file, io.TeeReader(res.Body, wc))
	if !errors.Is(errC, ErrTerminated) {
		checkErr(errC)
	}

	dl.downloadProgChan <- &UDLProgress{Percentage: 100, Done: true, File: info}

}

// UploadHdl is the handle returned by Upload
type UploadHdl struct {
	uploadProgChan chan *UDLProgress
	errChan        chan error
	commandChan    chan int8
	replyChan      chan int8
	metadata       *googledrive.File
	dest           string
	drive          *DriveClient
	isRunning      bool
}

// Upload a file
func (drive *DriveClient) Upload(metadata *googledrive.File, target string) *UploadHdl {
	ul := new(UploadHdl)
	ul.uploadProgChan = make(chan *UDLProgress, 5)
	ul.errChan = make(chan error, 5)
	ul.commandChan = make(chan int8)
	ul.replyChan = make(chan int8)
	ul.drive = drive
	ul.dest = target
	ul.metadata = metadata
	ul.isRunning = true
	go ul.upload()
	return ul
}

func (ul *UploadHdl) upload() {
	defer ul.onULError()
	defer func() {
		ul.isRunning = false
	}()
	content, errOpen := os.Open(path.Join(ul.drive.localRoot, ul.dest))
	checkErr(errOpen)
	stat, errStat := content.Stat()
	checkErr(errStat)
	wc := &writeCounter{totalBytes: stat.Size(),
		accu: 0,
		prog: ul.uploadProgChan}
	file, err := ul.drive.service.Files.Create(ul.metadata).
		EnforceSingleParent(true).Media(io.TeeReader(content, wc)).Do()
	if !errors.Is(err, ErrTerminated) {
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
func (drive *DriveClient) MkdirAll(target string) chan *MkDirProgress {
	var ch chan *MkDirProgress = make(chan *MkDirProgress, 10)
	go drive.mkdirall(target, ch)
	return ch
}

func (drive *DriveClient) mkdirall(target string, ch chan *MkDirProgress) {
	defer close(ch)
	defer onMkdirError(ch)

	var par []string
	ss := strings.Split(path.Dir(target), "/")[1:]
	if len(ss) > 1 || (len(ss) == 1 && ss[0] != "") {
		par = ss
	}
	dirname := path.Base(target)
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
