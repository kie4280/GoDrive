package godrive

import (
	"bufio"
	"encoding/json"
	"fmt"

	"errors"
	"github.com/oleiade/lane"
	"github.com/panjf2000/ants/v2"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"io/ioutil"
	"log"
	"net/http"
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
	errorChan          chan error
	resultChan         chan *ListResult
}

type foldBatch struct {
	ids           []string
	nextPageToken string
}

// ListResult of the command
type ListResult struct {
	Files   int
	Folders int
	err     error
}

func (drive *DriveClient) onError(err error) {

	if err != nil {
		var newError *ListResult = &ListResult{
			Files:   -1,
			Folders: -1,
			err:     errors.New("DriveClient error: " + err.Error() + string(debug.Stack()))}
		drive.resultChan <- newError
	}
}

// Retrieve a token, saves the token, then returns the generated client.fo
func getClient(config *oauth2.Config) *http.Client {
	// The file token.json stores the user's access and refresh tokens, and is
	// created automatically when the authorization flow completes for the first
	// time.
	tokFile := "./secrets/token.json"
	tok, err := tokenFromFile(tokFile)
	if err != nil {
		tok = getTokenFromWeb(config)
		saveToken(tokFile, tok)
	}
	return config.Client(context.Background(), tok)
}

// Request a token from the web, then returns the retrieved token.
func getTokenFromWeb(config *oauth2.Config) *oauth2.Token {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	fmt.Printf("Go to the following link in your browser then type the "+
		"authorization code: \n%v\n", authURL)

	var authCode string
	if _, err := fmt.Scan(&authCode); err != nil {
		debug.PrintStack()
		log.Fatalf("Unable to read authorization code %v", err)
	}

	tok, err := config.Exchange(context.TODO(), authCode)
	if err != nil {
		debug.PrintStack()
		log.Fatalf("Unable to retrieve token from web %v", err)
	}
	return tok
}

// Retrieves a token from a local file.
func tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	tok := &oauth2.Token{}
	err = json.NewDecoder(f).Decode(tok)
	return tok, err
}

// Saves a token to a file path.
func saveToken(path string, token *oauth2.Token) {
	fmt.Printf("Saving credential file to: %s\n", path)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		debug.PrintStack()
		log.Fatalf("Unable to cache oauth token: %v", err)
	}
	defer f.Close()
	json.NewEncoder(f).Encode(token)
}

// NewService creates a drive client
func newService() *drive.Service {

	b, err := ioutil.ReadFile("./secrets/client_secret_1.json")
	if err != nil {
		debug.PrintStack()
		log.Fatalf("Unable to read client secret file: %v", err)
	}

	// If modifying these scopes, delete your previously saved token.json.
	config, err := google.ConfigFromJSON(b, drive.DriveScope)
	if err != nil {
		debug.PrintStack()
		log.Fatalf("Unable to parse client secret file to config: %v", err)
	}
	client := getClient(config)
	srv, err := drive.New(client)

	if err != nil {
		debug.PrintStack()
		log.Fatalf("Unable to retrieve Drive client: %v", err)
	}

	return srv

}

func makeBatch(ids []string, nextPage string) *foldBatch {
	root := new(foldBatch)
	root.ids = ids
	root.nextPageToken = nextPage
	return root
}

// NewClient a new googledrive client
func NewClient() *DriveClient {
	client := new(DriveClient)
	client.service = newService()
	client.foldersearchQueue = lane.NewQueue()
	client.folderUnbatchSlice = make([]string, 0, 10*batchSize)
	client.canRun = true
	client.isRunning = false
	client.onGoingRequests = 0
	client.requestInterv = 20
	client.regRateLimit = regexp.MustCompile(userRateLimitExceed)
	client.filecount = 0
	client.foldcount = 0
	client.errorChan = make(chan error, 100)
	client.resultChan = make(chan *ListResult, 100)

	return client
}

// ListAll write a list of folders and files to "location". Returns ListResult struct
func (drive *DriveClient) ListAll(location string, rootID string) chan *ListResult {

	newService()
	go drive.listAll(location)
	return drive.resultChan
}

func (drive *DriveClient) listAll(location string) {

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
	ll = append(ll, "root")

	drive.foldersearchQueue.Enqueue(makeBatch(ll, ""))
	var foldChan chan []byte = make(chan []byte, 10000)
	var fileChan chan []byte = make(chan []byte, 10000)

	drive.filecount, drive.foldcount = 0, 0
	var workDone bool = drive.foldersearchQueue.Empty() && atomic.LoadInt32(&drive.onGoingRequests) == 0

	go drive.writeFiles(location, "folders.json", foldChan)
	go drive.writeFiles(location, "files.json", fileChan)
	go drive.Progress()

	for !workDone && drive.canRun {

		largeQueue := drive.foldersearchQueue.Size() > minWaitingBatch

		if largeQueue {
			for i := 0; i < maxGoroutine; i++ {
				atomic.AddInt32(&drive.onGoingRequests, 1)
				drive.unBatchMux.Lock()
				if len(drive.folderUnbatchSlice) >= batchSize {

					drive.foldersearchQueue.Enqueue(makeBatch(drive.folderUnbatchSlice[:batchSize], ""))
					drive.folderUnbatchSlice = drive.folderUnbatchSlice[batchSize:]
				}
				drive.unBatchMux.Unlock()

				drive.onError(p.Invoke([3]interface{}{foldChan, fileChan, drive.foldersearchQueue.Dequeue()}))
			}

		} else if !largeQueue && maxGoroutine-p.Free() <= minGoroutine {
			atomic.AddInt32(&drive.onGoingRequests, 1)
			drive.unBatchMux.Lock()
			if len(drive.folderUnbatchSlice) >= batchSize {

				drive.foldersearchQueue.Enqueue(makeBatch(drive.folderUnbatchSlice[:batchSize], ""))
				drive.folderUnbatchSlice = drive.folderUnbatchSlice[batchSize:]
			} else if len(drive.folderUnbatchSlice) > 0 {
				drive.foldersearchQueue.Enqueue(makeBatch(drive.folderUnbatchSlice, ""))
				drive.folderUnbatchSlice = make([]string, 0, 10*batchSize)
			}
			drive.unBatchMux.Unlock()
			drive.onError(p.Invoke([3]interface{}{foldChan, fileChan, drive.foldersearchQueue.Dequeue()}))
			time.Sleep(100 * time.Millisecond) // sleep longer
		}
		if atomic.LoadInt32(&drive.requestInterv) > 0 {
			atomic.AddInt32(&drive.requestInterv, -10)
		}

		time.Sleep(time.Duration(atomic.LoadInt32(&drive.requestInterv)) * time.Millisecond) // preventing exceed user rate limit
		drive.unBatchMux.Lock()
		workDone = drive.foldersearchQueue.Empty() && atomic.LoadInt32(&drive.onGoingRequests) == 0 && len(drive.folderUnbatchSlice) == 0
		drive.unBatchMux.Unlock()

	}
	drive.isRunning = false
	close(fileChan)
	close(foldChan)
	drive.resultChan <- &ListResult{Files: int(drive.filecount), Folders: int(drive.foldcount), err: nil}
	close(drive.resultChan)

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

	r, err := newService().Files.List().PageSize(1000).
		Fields(foldersearchFields).
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

func (drive *DriveClient) writeFiles(location string, filename string, outchan chan []byte) {
	foldpath := filepath.Join(location, ".GoDrive", "remote")
	errMk := os.MkdirAll(foldpath, 0777)
	file, err := os.Create(filepath.Join(foldpath, filename))
	writer := bufio.NewWriter(file)
	defer file.Close()

	if err != nil {

		fmt.Printf("Error creating file: %v\n", err)
	}
	if errMk != nil {

		fmt.Printf("Error creating directory: %v\n", errMk)
	}

	_, err1 := writer.WriteString("[")
	workdone := drive.foldersearchQueue.Empty() && atomic.LoadInt32(&drive.onGoingRequests) == 0
	var i []byte
	var ok bool = true
	for !workdone && drive.canRun && ok {
		i, ok = <-outchan
		for _, a := range i {
			err := writer.WriteByte(a)
			drive.onError(err)
		}

		if ok {
			e, err := writer.WriteString(",\n")
			_ = e
			drive.onError(err)
		}

	}

	_, err2 := writer.WriteString("]")
	err3 := writer.Flush()
	drive.onError(err1)
	drive.onError(err2)
	drive.onError(err3)

}

// Progress report current progress
func (drive *DriveClient) Progress() {
	workDone := drive.foldersearchQueue.Empty() && atomic.LoadInt32(&drive.onGoingRequests) == 0
	for !workDone && drive.canRun {
		fmt.Printf("request internal: %d files: %d folders: %d\n", atomic.LoadInt32(&drive.requestInterv), atomic.LoadInt32(&drive.filecount), atomic.LoadInt32(&drive.foldcount))
		time.Sleep(1 * time.Second)
		workDone = drive.foldersearchQueue.Empty() && atomic.LoadInt32(&drive.onGoingRequests) == 0
	}
}

// Cancel any ongoing operation
func (drive *DriveClient) Cancel() {
	drive.canRun = false

}
