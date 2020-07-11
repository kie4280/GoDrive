package godrive

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"container/list"
	"github.com/oleiade/lane"
	"github.com/panjf2000/ants/v2"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	service           *drive.Service = nil
	foldersearchQueue *lane.Queue    = nil
	filesearchQueue   *lane.Queue    = nil
	canRun            bool           = true
	isRunning         bool           = false
	onGoingRequests   int32          = 0
	requestInterv     int32          = 20
	foldqMux          sync.Mutex
	fileqMux          sync.Mutex
	foldwMux          sync.Mutex
	filewMux          sync.Mutex
	reRateLimit       *regexp.Regexp
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

type foldBatch struct {
	ids           *list.List
	nextPageToken string
}

// Retrieve a token, saves the token, then returns the generated client.
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
func NewService() *drive.Service {
	if service == nil {
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
		} else {
			service = srv
		}

	}

	return service

}

func makeBatch(ids *list.List, nextPage string) *foldBatch {
	root := new(foldBatch)
	root.ids = ids
	root.nextPageToken = nextPage
	return root
}

// ListAll write a list of folders and files to "location". Returns (folder count, file count)
func ListAll(location string) (int, int) {
	p, errP := ants.NewPoolWithFunc(maxGoroutine, recursiveFoldSearch)
	if errP != nil {
		log.Fatalf("There is a problem starting goroutines: %v", errP)
	}
	defer p.Release()
	onGoingRequests = 0
	reRateLimit = regexp.MustCompile(userRateLimitExceed)
	canRun = true
	isRunning = true
	foldersearchQueue = lane.NewQueue()
	ll := list.New()
	ll.PushBack("root")
	foldersearchQueue.Enqueue(makeBatch(ll, ""))
	var foldChan chan []byte = make(chan []byte, 1000)
	var fileChan chan []byte = make(chan []byte, 1000)
	var filecount, foldcount int32 = 0, 0

	var workDone bool = foldersearchQueue.Empty() && atomic.LoadInt32(&onGoingRequests) == 0
	for !workDone && canRun {

		largeQueue := foldersearchQueue.Size() > minWaitingBatch

		if largeQueue {
			for i := 0; i < maxGoroutine; i++ {
				atomic.AddInt32(&onGoingRequests, 1)
				p.Invoke([5]interface{}{foldChan, fileChan, foldersearchQueue.Dequeue(), &foldcount, &filecount})
			}

		} else if !largeQueue && maxGoroutine-p.Free() <= minGoroutine {
			atomic.AddInt32(&onGoingRequests, 1)
			p.Invoke([5]interface{}{foldChan, fileChan, foldersearchQueue.Dequeue(), &foldcount, &filecount})
			time.Sleep(10 * time.Millisecond) // sleep longer
		}
		if atomic.LoadInt32(&requestInterv) > 0 {
			atomic.AddInt32(&requestInterv, -10)
		}

		time.Sleep(time.Duration(atomic.LoadInt32(&requestInterv)) * time.Millisecond) // preventing exceed user rate limit

		workDone = foldersearchQueue.Empty() && atomic.LoadInt32(&onGoingRequests) == 0

	}
	isRunning = false

	return int(foldcount), int(filecount)
}

func recursiveFoldSearch(args interface{}) {
	unpackArgs := args.([5]interface{})
	writeFold := unpackArgs[0].(chan []byte)
	writeFile := unpackArgs[1].(chan []byte)
	_ = writeFile
	_ = writeFold
	batch, ok := unpackArgs[2].(*foldBatch)
	foldcount := unpackArgs[3].(*int32)
	filecount := unpackArgs[4].(*int32)
	if !ok || batch == nil {
		atomic.AddInt32(&onGoingRequests, -1)
		return
	}
	// fmt.Printf("recursiveFold\n")

	var str strings.Builder
	str.WriteString("(")
	for i := batch.ids.Front(); i != nil; i = i.Next() {
		str.WriteString("'")
		str.WriteString(i.Value.(string))
		str.WriteString("' in parents")
		if i.Next() != nil {
			str.WriteString(" or ")
		}
	}
	str.WriteString(") and trashed=false")
	// fmt.Printf("string buffer: %s\n", str.String())

	r, err := NewService().Files.List().PageSize(1000).
		Fields(foldersearchFields).
		Q(str.String()).PageToken(batch.nextPageToken).
		Spaces("drive").Corpora("user").Do()
	if err != nil {
		fmt.Printf("google api error: %v", err)
		foldersearchQueue.Enqueue(batch)
		match := reRateLimit.FindString(err.Error())
		if match != "" {
			atomic.AddInt32(&requestInterv, 200)
			atomic.AddInt32(&onGoingRequests, -1)
			return
		}
		log.Fatalf("google api unexpected error: %v", err)

	}

	if r.NextPageToken != "" {
		batch.nextPageToken = r.NextPageToken
		foldersearchQueue.Enqueue(batch)
	}
	ll := list.New()

	for _, file := range r.Files {
		if file.MimeType == "application/vnd.google-apps.folder" {
			ll.PushBack(file.Id)
			tt, err := file.MarshalJSON()
			_ = err
			fmt.Printf("folder: %s \n", string(tt))
			atomic.AddInt32(foldcount, 1)
		} else {
			tt, err := file.MarshalJSON()
			_ = err
			_ = tt
			atomic.AddInt32(filecount, 1)
		}

		if ll.Len() > batchSize {
			foldersearchQueue.Enqueue(makeBatch(ll, ""))
			ll = list.New()
		}
	}
	if ll.Len() > 0 {
		foldersearchQueue.Enqueue(makeBatch(ll, ""))

	}

	atomic.AddInt32(&onGoingRequests, -1)

}

func writeFiles(location string, filename string, outchan chan []byte) {
	foldpath := filepath.Join(location, ".GoDrive")
	errMk := os.MkdirAll(foldpath, 0666)
	file, err := os.Create(filepath.Join(foldpath, filename))
	writer := bufio.NewWriter(file)
	defer file.Close()

	if err != nil {
		debug.PrintStack()
		fmt.Printf("Error creating file: %v", err)
	}
	if errMk != nil {
		debug.PrintStack()
		fmt.Printf("Error creating directory: %v", errMk)
	}

	for isRunning {
		for _, i := range <-outchan {
			writer.WriteByte(i)
		}

	}

	writer.Flush()

}

// Cancel any ongoing operation
func Cancel() {
	canRun = false

}
