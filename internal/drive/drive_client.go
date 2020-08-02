package drive

import (
	"bufio"
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
	writeWait          sync.WaitGroup
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

func (drive *DriveClient) onError(err error) {

	if err != nil {
		var newError *Progress = &Progress{
			Files:   -1,
			Folders: -1,
			Error: errors.New("DriveClient error: " + err.Error() +
				"\n" + string(debug.Stack())),
			Done: false}
		drive.progressChan <- newError
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

	client.isRunning = false
	client.canRun = false
	client.onGoingRequests = 0
	client.requestInterv = 20
	client.regRateLimit = regexp.MustCompile(userRateLimitExceed)
	client.filecount = 0
	client.foldcount = 0
	client.localRoot = localDir
	client.remoteRootID = remoteID
	if err != nil {
		return nil, err
	}

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
		log.Fatalf("There is a problem starting goroutines: %v", errP)
	}
	defer p.Release()
	drive.onGoingRequests = 0
	drive.canRun = true
	drive.isRunning = true
	drive.foldersearchQueue = lane.NewQueue()
	drive.folderUnbatchSlice = make([]string, 0, batchSize*10)
	ll := make([]string, 0, 1)
	ll = append(ll, drive.remoteRootID)

	drive.foldersearchQueue.Enqueue(makeBatch(ll, ""))
	var foldChan chan []byte = make(chan []byte, 10000)
	var fileChan chan []byte = make(chan []byte, 10000)

	drive.filecount, drive.foldcount = 0, 0
	var workDone bool = false

	go drive.writeFiles("folders.json", foldChan)
	go drive.writeFiles("files.json", fileChan)
	drive.writeWait.Add(2)

	go drive.ShowProgress()

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

				drive.onError(p.Invoke([3]interface{}{foldChan, fileChan,
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
			drive.onError(p.Invoke([3]interface{}{foldChan, fileChan,
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
	drive.isRunning = false
	close(fileChan)
	close(foldChan)
	drive.writeWait.Wait()
	drive.progressChan <- &Progress{Files: int(drive.filecount),
		Folders: int(drive.foldcount), Error: nil, Done: true}
	close(drive.progressChan)

}

func (drive *DriveClient) recursiveFoldSearch(args interface{}) {
	unpackArgs := args.([3]interface{})
	writeFold := unpackArgs[0].(chan []byte)
	writeFile := unpackArgs[1].(chan []byte)
	_ = writeFile
	_ = writeFold
	batch, ok := unpackArgs[2].(*foldBatch)
	if !ok || len(batch.ids) == 0 {
		atomic.AddInt32(&drive.onGoingRequests, -1)
		return
	}
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
		log.Fatalf("google api unexpected error: %v\n", err)

	}

	if r.NextPageToken != "" {
		batch.nextPageToken = r.NextPageToken
		drive.foldersearchQueue.Enqueue(batch)
	}
	ll := make([]string, 0, batchSize)

	for _, file := range r.Files {
		if file.MimeType == "application/vnd.google-apps.folder" {
			ll = append(ll, file.Id)
			tt, err := file.MarshalJSON()
			drive.onError(err)
			writeFold <- tt
			// fmt.Printf("folder: %s \n", string(tt)) // print out folder json
			atomic.AddInt32(&drive.foldcount, 1)
		} else {
			tt, err := file.MarshalJSON()
			drive.onError(err)
			writeFile <- tt
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

func (drive *DriveClient) writeFiles(filename string, outchan chan []byte) {
	foldpath := filepath.Join(drive.localRoot, ".GoDrive", "remote")
	errMk := os.MkdirAll(foldpath, 0777)
	drive.onError(errMk)
	file, err := os.Create(filepath.Join(foldpath, filename))
	drive.onError(err)
	writer := bufio.NewWriter(file)
	defer file.Close()

	_, err1 := writer.WriteString("[")
	drive.onError(err1)
	var i []byte
	var ok bool = true
	i, ok = <-outchan
	for drive.canRun && ok {

		for _, a := range i {
			err := writer.WriteByte(a)
			drive.onError(err)
		}
		i, ok = <-outchan
		if ok {
			e, err := writer.WriteString(",\n")
			_ = e
			drive.onError(err)
		}

	}

	_, err2 := writer.WriteString("]")
	drive.onError(err2)
	err3 := writer.Flush()
	drive.onError(err3)
	drive.writeWait.Done()

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
