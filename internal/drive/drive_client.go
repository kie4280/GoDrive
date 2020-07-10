package godrive

import (
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
	// "regexp"
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
)

const (
	foldersearchFields = "nextPageToken, files(id, name, mimeType, modifiedTime, parents)"
	filesearchFields   = "nextPageToken, files(id, name, mimeType, modifiedTime, md5Checksum, parents)"
	maxGoroutine       = 10
	minGoroutine       = 2
	batchSize          = 300
	minWaitingBatch    = 20
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

// NewService creates drive clients
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

// ListAll write a list of folders to "location"
func ListAll(location string) *drive.FileList {
	p, errP := ants.NewPoolWithFunc(maxGoroutine, recursiveFoldSearch)
	if errP != nil {
		log.Fatalf("There is a problem starting goroutines: %v", errP)
	}
	defer p.Release()
	onGoingRequests = 0
	canRun = true
	isRunning = true
	foldersearchQueue = lane.NewQueue()
	ll := list.New()
	ll.PushBack("root")
	foldersearchQueue.Enqueue(makeBatch(ll, ""))
	var foldChan chan []byte = make(chan []byte, 1000)
	var fileChan chan []byte = make(chan []byte, 1000)

	var workDone bool = foldersearchQueue.Empty() && atomic.LoadInt32(&onGoingRequests) == 0
	for !workDone && canRun {

		largeQueue := foldersearchQueue.Size() > minWaitingBatch
		atomic.AddInt32(&onGoingRequests, 1)

		if largeQueue {
			for i := 0; i < maxGoroutine; i++ {
				p.Invoke([3]interface{}{foldChan, fileChan, foldersearchQueue.Dequeue()})
			}

		} else if !largeQueue && maxGoroutine-p.Free() <= minGoroutine {
			p.Invoke([3]interface{}{foldChan, fileChan, foldersearchQueue.Dequeue()})
			time.Sleep(100 * time.Millisecond) // sleep longer
		}
		atomic.AddInt32(&requestInterv, -10)

		time.Sleep(time.Duration(atomic.LoadInt32(&requestInterv)) * time.Millisecond) // preventing exceed user rate limit

		workDone = foldersearchQueue.Empty() && atomic.LoadInt32(&onGoingRequests) == 0

	}
	isRunning = false

	return new(drive.FileList)
}

func recursiveFoldSearch(args interface{}) {
	unpackArgs := args.([3]interface{})
	writeFold := unpackArgs[0].(chan []byte)
	writeFile := unpackArgs[1].(chan []byte)
	_ = writeFile
	_ = writeFold
	batch, ok := unpackArgs[2].(*foldBatch)
	if !ok || batch == nil {
		atomic.AddInt32(&onGoingRequests, -1)
		return
	}
	fmt.Printf("recursiveFold\n")

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
	fmt.Printf("string buffer: %s\n", str.String())
	r, err := NewService().Files.List().PageSize(1000).
		Fields(foldersearchFields).
		Q(str.String()).
		Spaces("drive").Corpora("user").Do()
	if err != nil {
		fmt.Printf("Retrieve error: %v", err)
		atomic.AddInt32(&requestInterv, 200)
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
			fmt.Printf("file: %s \n", string(tt))
		} else {
			tt, err := file.MarshalJSON()
			_ = err
			_ = tt
		}

		if ll.Len() > batchSize {
			foldersearchQueue.Enqueue(makeBatch(ll, ""))
			ll.Init()
		}
	}
	if ll.Len() > 0 {
		foldersearchQueue.Enqueue(makeBatch(ll, ""))

	}

	atomic.AddInt32(&onGoingRequests, -1)

}

func writeFiles(location string, files chan []byte, folders chan []byte) {
	errMk := os.MkdirAll(location+"/.GoDrive/", 0666)
	folder, err1 := os.Create(location + "/.GoDrive/folders.json")
	file, err2 := os.Create(location + "/.GoDrive/files.json")
	defer folder.Close()
	defer file.Close()
	if err1 != nil {
		debug.PrintStack()
		log.Fatalf("Error creating file: %v", err1)
	}
	if err2 != nil {
		debug.PrintStack()
		log.Fatalf("Error creating file: %v", err2)
	}
	if errMk != nil {
		debug.PrintStack()
		log.Fatalf("Error creating directory: %v", errMk)
	}

	for isRunning {

	}

}

// Cancel any ongoing operation
func Cancel() {
	canRun = false

}
