package gdrive

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/oleiade/lane"
	"github.com/panjf2000/ants/v2"
	"godrive/internal/googleclient"
	"google.golang.org/api/drive/v3"
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
	foldersearchFields  = "nextPageToken, files(id, name, mimeType, modifiedTime, parents)"
	filesearchFields    = "nextPageToken, files(id, name, mimeType, modifiedTime, md5Checksum, parents)"
	maxGoroutine        = 10
	minGoroutine        = 2
	batchSize           = 100
	minWaitingBatch     = 4
	userRateLimitExceed = "User Rate Limit Exceeded"
)

// DriveClient represents a google drive client object
type DriveClient struct {
	service            *drive.Service
	foldersearchQueue  *lane.Queue
	folderUnbatchSlice []string
	unBatchMux         sync.Mutex
	fileMapMux         sync.Mutex
	foldMapMux         sync.Mutex
	canRun             bool
	isRunning          bool
	onGoingRequests    int32
	requestInterv      int32
	regRateLimit       *regexp.Regexp
	filecount          int32
	foldcount          int32
	progressChan       chan *Progress
	localRoot          string
	remoteRootID       string
	driveFileMap       map[string]*fileHolder
	driveFoldMap       map[string]*foldHolder
}

type foldBatch struct {
	ids           []string
	nextPageToken string
}

// Progress of the command
type Progress struct {
	Files   int
	Folders int
	Done    bool
	Error   error
}

func (drive *DriveClient) checkErr(err error) {

	if err != nil {
		var newError *Progress = &Progress{
			Files:   -1,
			Folders: -1,
			Error: errors.New("DriveClient error: " + err.Error() +
				"\n" + string(debug.Stack())),
			Done: false}
		drive.progressChan <- newError
		panic(err)
	}
}

func onError() {
	if err := recover(); err != nil {
		log.Printf("DriveClient error: %v", err)
	}
}

// Cancel any ongoing operation
func (drive *DriveClient) Cancel() {
	drive.canRun = false

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

	client.isRunning = false
	client.canRun = false
	client.onGoingRequests = 0
	client.requestInterv = 20
	client.regRateLimit = regexp.MustCompile(userRateLimitExceed)
	client.filecount = 0
	client.foldcount = 0
	client.localRoot = localDir
	client.remoteRootID = remoteID

	return client, nil
}

// ListAll write a list of folders and files to "location". Returns Progress struct
func (drive *DriveClient) ListAll() chan *Progress {

	drive.progressChan = make(chan *Progress, 10)
	go drive.listAll()
	return drive.progressChan
}

func (drive *DriveClient) listAll() {

	p, errP := ants.NewPoolWithFunc(maxGoroutine, drive.recursiveFoldSearch)
	if errP != nil {
		log.Fatalf("There is a problem starting goroutine pool: %v", errP)
	}
	defer p.Release()
	defer onError()
	drive.onGoingRequests = 0
	drive.canRun = true
	drive.isRunning = true
	drive.foldersearchQueue = lane.NewQueue()
	drive.folderUnbatchSlice = make([]string, 0, batchSize*10)
	ll := make([]string, 0, 1)
	ll = append(ll, drive.remoteRootID)

	drive.foldersearchQueue.Enqueue(makeBatch(ll, ""))

	drive.filecount, drive.foldcount = 0, 0
	var workDone bool = false
	drive.driveFileMap = make(map[string]*fileHolder)
	drive.driveFoldMap = make(map[string]*foldHolder)

	go drive.ShowProgress()

	defer func() {

		drive.progressChan <- &Progress{Files: int(drive.filecount),
			Folders: int(drive.foldcount), Error: nil, Done: true}
		close(drive.progressChan)
	}()

	for !workDone && drive.canRun {

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

				drive.checkErr(p.Invoke([1]interface{}{
					drive.foldersearchQueue.Dequeue()}))
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
				drive.folderUnbatchSlice = make([]string, 0, 10*batchSize)
			}
			drive.unBatchMux.Unlock()
			drive.checkErr(p.Invoke([1]interface{}{
				drive.foldersearchQueue.Dequeue()}))
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
	drive.writeFolds("folders.json")
	drive.writeFiles("files.json")
	drive.isRunning = false

}

func (drive *DriveClient) recursiveFoldSearch(args interface{}) {
	unpackArgs := args.([1]interface{})

	batch, ok := unpackArgs[0].(*foldBatch)
	if !ok || len(batch.ids) == 0 {
		atomic.AddInt32(&drive.onGoingRequests, -1)
		return
	}
	defer onError()
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
		Fields(filesearchFields).
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
		drive.checkErr(err)

	}

	if r.NextPageToken != "" {
		batch.nextPageToken = r.NextPageToken
		drive.foldersearchQueue.Enqueue(batch)
	}
	ll := make([]string, 0, batchSize)

	for _, file := range r.Files {

		if file.MimeType == "application/vnd.google-apps.folder" {
			ll = append(ll, file.Id)
			drive.foldMapMux.Lock()
			drive.driveFoldMap[file.Id] = convFolStruct(file)
			drive.foldMapMux.Unlock()
			// fmt.Printf("folder: %s \n", string(tt)) // print out folder json
			atomic.AddInt32(&drive.foldcount, 1)
		} else {
			drive.fileMapMux.Lock()
			drive.driveFileMap[file.Id] = convFilStruct(file)
			drive.fileMapMux.Unlock()
			atomic.AddInt32(&drive.filecount, 1)
		}

		drive.foldMapMux.Lock()
		for _, ff := range file.Parents {
			par, ok := drive.driveFoldMap[ff]
			if ok {
				par.Children = append(par.Children, file.Id)
			}
		}
		drive.foldMapMux.Unlock()

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
}

type foldHolder struct {
	Name     string
	MimeType string
	ModTime  string
	Parents  []string
	Children []string
}

func convFolStruct(file *drive.File) *foldHolder {
	aa := new(foldHolder)
	aa.Name = file.Name
	aa.MimeType = file.MimeType
	aa.ModTime = file.ModifiedTime
	aa.Parents = file.Parents
	return aa
}

func convFilStruct(file *drive.File) *fileHolder {
	aa := new(fileHolder)
	aa.Name = file.Name
	aa.MimeType = file.MimeType
	aa.ModTime = file.ModifiedTime
	aa.Parents = file.Parents
	aa.Md5Chk = file.Md5Checksum
	return aa
}

func (drive *DriveClient) writeFiles(filename string) {
	defer onError()
	foldpath := filepath.Join(drive.localRoot, ".GoDrive", "remote")
	errMk := os.MkdirAll(foldpath, 0777)
	drive.checkErr(errMk)
	file, err := os.Create(filepath.Join(foldpath, filename))
	drive.checkErr(err)

	defer file.Close()

	err = json.NewEncoder(file).Encode(drive.driveFileMap)
	drive.checkErr(err)

}

func (drive *DriveClient) writeFolds(filename string) {
	defer onError()
	foldpath := filepath.Join(drive.localRoot, ".GoDrive", "remote")
	errMk := os.MkdirAll(foldpath, 0777)
	drive.checkErr(errMk)
	file, err := os.Create(filepath.Join(foldpath, filename))
	drive.checkErr(err)

	defer file.Close()

	err = json.NewEncoder(file).Encode(drive.driveFoldMap)
	drive.checkErr(err)

}

// ShowProgress report current progress
func (drive *DriveClient) ShowProgress() {
	drive.unBatchMux.Lock()
	workDone := drive.foldersearchQueue.Empty() &&
		atomic.LoadInt32(&drive.onGoingRequests) == 0 &&
		len(drive.folderUnbatchSlice) == 0
	drive.unBatchMux.Unlock()
	for !workDone && drive.canRun {
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
