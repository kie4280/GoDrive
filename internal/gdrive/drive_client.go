package gdrive

import (
	"encoding/json"
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
	maxGoroutine        = 15
	minGoroutine        = 2
	batchSize           = 100
	minWaitingBatch     = 4
	userRateLimitExceed = "User Rate Limit Exceeded"
	// C_CANCEL cancels the listAll operation
	C_CANCEL int8 = 1
)

// DriveClient represents a google drive client object
type DriveClient struct {
	service            *googledrive.Service
	foldersearchQueue  *lane.Queue
	folderUnbatchSlice []string
	unBatchMux         sync.Mutex
	fileMapMux         sync.Mutex
	foldMapMux         sync.Mutex
	pathMapMux         sync.Mutex
	canRunList         bool
	isListRunning      bool
	onGoingRequests    int32
	requestInterv      int32
	regRateLimit       *regexp.Regexp
	filecount          int32
	foldcount          int32
	localRoot          string
	remoteRootID       string
	driveFileMap       map[string]*fileHolder
	driveFoldMap       map[string]*foldHolder
	pathMap            map[string]string
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

func onListError(progressChan chan *ListProgress) {
	if err := recover(); err != nil {
		err1 := err.(error)
		var newError *ListProgress = &ListProgress{
			Files:   -1,
			Folders: -1,
			Error: errors.New("DriveClient list error: " + err1.Error() +
				"\n" + string(debug.Stack())),
			Done: false}
		progressChan <- newError
		log.Printf("DriveClient list error: %v", err1)
	}
}

func makeBatch(ids []string, nextPage string) *foldBatch {
	root := new(foldBatch)
	root.ids = ids
	root.nextPageToken = nextPage
	return root
}

// NewClient a new googledrive client (localDirPath, remoteRootID)
func NewClient(localDir string, remoteID string) (*DriveClient, error) {
	client := new(DriveClient)
	var err error
	client.service, err = googleclient.NewService(0)
	if err != nil {
		return nil, err
	}

	client.isListRunning = false
	client.onGoingRequests = 0
	client.requestInterv = 20
	client.regRateLimit = regexp.MustCompile(userRateLimitExceed)
	client.filecount = 0
	client.foldcount = 0
	client.localRoot = localDir
	client.remoteRootID = remoteID

	return client, nil
}

// ListProgress of the command
type ListProgress struct {
	Files   int
	Folders int
	Done    bool
	Error   error
	Command int8
}

func getCommands(bb interface{}) int8 {

	switch v := bb.(type) {
	case chan *ListProgress:
		select {
		case b := <-v:
			return b.Command

		default:
			return 0
		}
	case chan *UDLProgress:
		select {
		case b := <-v:
			return b.Command

		default:
			return 0
		}
	case chan *MkDirProgress:
		select {
		case b := <-v:
			return b.Command

		default:
			return 0
		}
	default:
		panic(errors.New("undefined type"))
	}

}

// ListAll write a list of folders and files to "location". Returns ListProgress struct if not already running, else returns nil
func (drive *DriveClient) ListAll() chan *ListProgress {
	if drive.isListRunning {
		return nil
	}
	progressChan := make(chan *ListProgress, 10)
	go drive.listAll(progressChan)
	return progressChan
}

func (drive *DriveClient) listAll(progressChan chan *ListProgress) {

	p, errP := ants.NewPoolWithFunc(maxGoroutine, drive.recursiveFoldSearch)
	if errP != nil {
		log.Fatalf("There is a problem starting goroutine pool: %v", errP)
	}
	defer close(progressChan)
	defer p.Release()
	defer onListError(progressChan)
	drive.onGoingRequests = 0
	drive.canRunList = true
	drive.isListRunning = true
	drive.foldersearchQueue = lane.NewQueue()
	drive.folderUnbatchSlice = make([]string, 0, batchSize)
	drive.pathMap = make(map[string]string, 10000)
	ll := make([]string, 0, 1)
	ll = append(ll, drive.remoteRootID)

	drive.foldersearchQueue.Enqueue(makeBatch(ll, ""))

	drive.filecount, drive.foldcount = 0, 0
	var workDone bool = false
	drive.driveFileMap = make(map[string]*fileHolder)
	drive.driveFoldMap = make(map[string]*foldHolder)

	go drive.ShowListProgress()

	for !workDone {
		if a := getCommands(progressChan); a > 0 {
			if a == C_CANCEL {
				drive.canRunList = false
				return
			}
		}
		largeQueue := drive.foldersearchQueue.Size() > minWaitingBatch

		if largeQueue {
			for i := 0; i < maxGoroutine; i++ {
				atomic.AddInt32(&drive.onGoingRequests, 1)
				drive.unBatchMux.Lock()
				if len(drive.folderUnbatchSlice) >= batchSize {

					drive.foldersearchQueue.Enqueue(
						makeBatch(drive.folderUnbatchSlice[:batchSize], ""))
					drive.folderUnbatchSlice = drive.folderUnbatchSlice[batchSize:]
				}
				drive.unBatchMux.Unlock()

				checkErr(p.Invoke([2]interface{}{
					drive.foldersearchQueue.Dequeue(), progressChan}))
			}

		} else if !largeQueue && maxGoroutine-p.Free() <= minGoroutine {
			atomic.AddInt32(&drive.onGoingRequests, 1)
			drive.unBatchMux.Lock()
			if len(drive.folderUnbatchSlice) >= batchSize {

				drive.foldersearchQueue.Enqueue(
					makeBatch(drive.folderUnbatchSlice[:batchSize], ""))
				drive.folderUnbatchSlice = drive.folderUnbatchSlice[batchSize:]
			} else if len(drive.folderUnbatchSlice) > 0 {
				drive.foldersearchQueue.Enqueue(makeBatch(drive.folderUnbatchSlice, ""))
				drive.folderUnbatchSlice = make([]string, 0, batchSize)
			}
			drive.unBatchMux.Unlock()
			checkErr(p.Invoke([2]interface{}{
				drive.foldersearchQueue.Dequeue(), progressChan}))
			time.Sleep(100 * time.Millisecond) // sleep longer
		}
		if atomic.LoadInt32(&drive.requestInterv) > 0 {
			atomic.AddInt32(&drive.requestInterv, -10)
		}

		time.Sleep(time.Duration(atomic.LoadInt32(&drive.requestInterv)) *
			time.Millisecond) // preventing exceed user rate limit
		drive.unBatchMux.Lock()
		workDone = drive.foldersearchQueue.Empty() &&
			atomic.LoadInt32(&drive.onGoingRequests) == 0 &&
			len(drive.folderUnbatchSlice) == 0
		drive.unBatchMux.Unlock()

	}
	drive.writeFolds("folders.json", "foldIDmap.json", progressChan)
	drive.writeFiles("files.json", progressChan)
	drive.isListRunning = false
	progressChan <- &ListProgress{Files: int(drive.filecount),
		Folders: int(drive.foldcount), Error: nil, Done: drive.canRunList}

}

func (drive *DriveClient) recursiveFoldSearch(args interface{}) {
	unpackArgs := args.([2]interface{})
	progressChan := unpackArgs[1].(chan *ListProgress)
	batch, ok := unpackArgs[0].(*foldBatch)
	if !ok || len(batch.ids) == 0 {
		atomic.AddInt32(&drive.onGoingRequests, -1)
		return
	}
	defer onListError(progressChan)
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

		match := drive.regRateLimit.FindString(err.Error())
		if match != "" {
			drive.foldersearchQueue.Enqueue(batch)
			atomic.AddInt32(&drive.requestInterv, 200)
			atomic.AddInt32(&drive.onGoingRequests, -1)
			fmt.Printf("rate limit: %v\n", err)
			return
		}
		checkErr(err)

	}

	if r.NextPageToken != "" {
		batch.nextPageToken = r.NextPageToken
		drive.foldersearchQueue.Enqueue(batch)
	}
	ll := make([]string, 0, batchSize)

	var parentPath string
	if len(r.Files) > 0 {
		drive.foldMapMux.Lock()
		fol, ok := drive.driveFoldMap[r.Files[0].Parents[0]]
		if ok {
			parentPath = filepath.Join(fol.Dir, fol.Name)
		} else {
			parentPath = "/"
			drive.pathMapMux.Lock()
			drive.pathMap["/"] = r.Files[0].Parents[0]
			drive.pathMapMux.Unlock()
		}

		drive.foldMapMux.Unlock()
	}

	for _, file := range r.Files {

		if file.MimeType == "application/vnd.google-apps.folder" {
			ll = append(ll, file.Id)
			drive.foldMapMux.Lock()
			drive.driveFoldMap[file.Id] = convFolStruct(file, parentPath)
			drive.foldMapMux.Unlock()
			drive.pathMapMux.Lock()
			drive.pathMap[filepath.Join(parentPath, file.Name)] = file.Id
			drive.pathMapMux.Unlock()
			atomic.AddInt32(&drive.foldcount, 1)
		} else {
			drive.fileMapMux.Lock()
			drive.driveFileMap[file.Id] = convFilStruct(file, parentPath)
			drive.fileMapMux.Unlock()
			atomic.AddInt32(&drive.filecount, 1)
		}

		if len(ll) >= batchSize {
			drive.foldersearchQueue.Enqueue(makeBatch(ll, ""))
			ll = make([]string, 0, batchSize)
		}
	}
	if len(ll) > 0 {
		drive.unBatchMux.Lock()
		drive.folderUnbatchSlice = append(drive.folderUnbatchSlice, ll...)
		drive.unBatchMux.Unlock()
		// drive.foldersearchQueue.Enqueue(makeBatch(ll, ""))
	}

	atomic.AddInt32(&drive.onGoingRequests, -1)

}

type fileHolder struct {
	Name     string
	MimeType string
	ModTime  string
	Parents  []string
	Md5Chk   string
	Dir      string
}

type foldHolder struct {
	Name     string
	MimeType string
	ModTime  string
	Parents  []string
	Dir      string
}

func convFolStruct(file *googledrive.File, path string) *foldHolder {
	aa := new(foldHolder)
	aa.Name = file.Name
	aa.MimeType = file.MimeType
	aa.ModTime = file.ModifiedTime
	aa.Parents = file.Parents
	aa.Dir = path
	return aa
}

func convFilStruct(file *googledrive.File, path string) *fileHolder {
	aa := new(fileHolder)
	aa.Name = file.Name
	aa.MimeType = file.MimeType
	aa.ModTime = file.ModifiedTime
	aa.Parents = file.Parents
	aa.Md5Chk = file.Md5Checksum
	aa.Dir = path
	return aa
}

func (drive *DriveClient) writeFiles(filename string, progressChan chan *ListProgress) {
	defer onListError(progressChan)
	foldpath := filepath.Join(drive.localRoot, ".GoDrive", "remote")
	errMk := os.MkdirAll(foldpath, 0777)
	checkErr(errMk)

	file, err := os.Create(filepath.Join(foldpath, filename))
	checkErr(err)
	defer file.Close()
	err = json.NewEncoder(file).Encode(drive.driveFileMap)
	checkErr(err)

}

func (drive *DriveClient) writeFolds(foldList string, foldIDmap string, progressChan chan *ListProgress) {
	defer onListError(progressChan)
	foldpath := filepath.Join(drive.localRoot, ".GoDrive", "remote")
	errMk := os.MkdirAll(foldpath, 0777)
	checkErr(errMk)

	list, err1 := os.Create(filepath.Join(foldpath, foldList))
	checkErr(err1)
	defer list.Close()
	err1 = json.NewEncoder(list).Encode(drive.driveFoldMap)
	checkErr(err1)

	Ids, err2 := os.Create(filepath.Join(foldpath, foldIDmap))
	checkErr(err2)
	defer Ids.Close()
	drive.pathMapMux.Lock()
	err2 = json.NewEncoder(Ids).Encode(drive.pathMap)
	drive.pathMapMux.Unlock()

	checkErr(err2)

}

// ShowListProgress report current progress
func (drive *DriveClient) ShowListProgress() {
	drive.unBatchMux.Lock()
	workDone := drive.foldersearchQueue.Empty() &&
		atomic.LoadInt32(&drive.onGoingRequests) == 0 &&
		len(drive.folderUnbatchSlice) == 0
	drive.unBatchMux.Unlock()
	for !workDone && drive.canRunList {
		fmt.Printf("request internal: %d files: %d folders: %d\n",
			atomic.LoadInt32(&drive.requestInterv),
			atomic.LoadInt32(&drive.filecount),
			atomic.LoadInt32(&drive.foldcount))
		time.Sleep(1 * time.Second)
		drive.unBatchMux.Lock()
		workDone = drive.foldersearchQueue.Empty() &&
			atomic.LoadInt32(&drive.onGoingRequests) == 0 &&
			len(drive.folderUnbatchSlice) == 0
		drive.unBatchMux.Unlock()
	}
}

// UDLProgress represents the download progress
type UDLProgress struct {
	Percentage float32
	Done       bool
	Err        error
	File       *googledrive.File
	Command    int8
}

type writeCounter struct {
	totalBytes int64
	accu       int64
	prog       chan *UDLProgress
}

func (wc *writeCounter) Write(p []byte) (int, error) {
	n := len(p)
	wc.accu += int64(n)
	if wc.totalBytes == 0 {
		wc.prog <- &UDLProgress{Percentage: -1, Done: false, Err: nil, File: nil}
	} else {
		wc.prog <- &UDLProgress{Percentage: float32(wc.accu) / float32(wc.totalBytes) * 100,
			Done: false, Err: nil, File: nil}
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
	checkErr(errC)
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
	checkErr(err)
	ch <- &UDLProgress{Percentage: 100, Done: true, Err: nil, File: file}
}

// MkDirProgress represents the mkdir progress
type MkDirProgress struct {
	Done    bool
	Err     error
	File    *googledrive.File
	Command int8
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
