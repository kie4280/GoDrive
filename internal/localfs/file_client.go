package localfs

import (
	"bufio"
	"crypto/md5"
	"encoding/json"

	// "fmt"
	"github.com/oleiade/lane"
	"github.com/panjf2000/ants/v2"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"sync"
	"sync/atomic"
	"time"
)

const (
	maxGoroutine = 10
	minGoroutine = 2
)

// LocalClient indexes files
type LocalClient struct {
	progressChan    chan *Progress
	rootDir         string
	folderQueue     *lane.Queue
	foldQMux        sync.Mutex
	canRun          bool
	filecount       int32
	foldcount       int32
	onGoingRequests int32
	writeWait       sync.WaitGroup
}

// File represents a file on a filesystem
type File struct {
	ID      []byte
	Name    string
	Md5sum  []byte
	Parents []string
	Modtime string
	Isdir   bool
}

// Progress of the command
type Progress struct {
	Files   int
	Folders int
	Done    bool
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

// Cancel the current operation
func (fw *LocalClient) Cancel() {
	fw.canRun = false
}

// NewClient returns a new LocalClient object
func NewClient(rootDir string) *LocalClient {
	ws := new(LocalClient)
	ws.rootDir = rootDir
	ws.canRun = false
	return ws
}

// Hashsum returns the md5 hash of "file" with path relative to rootDir
func (fw *LocalClient) hashsum(path string, info os.FileInfo) *File {
	filename := filepath.Base(path)
	abspath := filepath.Join(fw.rootDir, path)
	relpath := filepath.Clean(path)

	if info.IsDir() {
		h1 := md5.New()
		io.WriteString(h1, relpath+"/")

		return &File{
			Name: filename, ID: h1.Sum(nil), Isdir: true,
			Modtime: info.ModTime().UTC().Format(time.RFC3339)}

	}
	f, openerr := os.Open(abspath)
	checkErr(openerr)
	defer f.Close()
	if openerr == nil {
		h1 := md5.New()
		_, copyerr := io.Copy(h1, f)
		checkErr(copyerr)
		h2 := md5.New()
		io.WriteString(h2, relpath)

		return &File{
			Name: filename, ID: h2.Sum(nil), Isdir: false, Md5sum: h1.Sum(nil),
			Modtime: info.ModTime().UTC().Format(time.RFC3339)}
	}

	return nil

}

// ListAll lists the folders and files below "location"
func (fw *LocalClient) ListAll() chan *Progress {
	fw.progressChan = make(chan *Progress, 10)
	go fw.listAll()
	return fw.progressChan
}

func (fw *LocalClient) listAll() {
	p, errP := ants.NewPoolWithFunc(maxGoroutine, fw.recursiveFoldsearch)
	if errP != nil {
		log.Fatalf("There is a problem starting goroutines: %v", errP)
	}
	defer p.Release()
	fw.onGoingRequests = 0
	fw.filecount, fw.foldcount = 0, 0
	fw.folderQueue = lane.NewQueue()
	fw.canRun = true
	var foldChan chan *File = make(chan *File, 10000)
	var fileChan chan *File = make(chan *File, 10000)

	go fw.writeFiles("folders.json", foldChan)
	go fw.writeFiles("files.json", fileChan)
	fw.writeWait.Add(2)

	fol, listErr := ioutil.ReadDir(fw.rootDir)
	if listErr != nil {
		log.Fatalf("Cannot read directory: %v", listErr)
	}
	for _, i := range fol {
		if i.Name() != ".GoDrive" {

			if i.IsDir() {
				fw.folderQueue.Enqueue("/" + i.Name())
				foldChan <- fw.hashsum("/"+i.Name(), i)
			} else {
				fileChan <- fw.hashsum("/"+i.Name(), i)
			}

		}

	}

	var workDone bool = false

	for !workDone && fw.canRun {

		if p.Free() > 0 && !fw.folderQueue.Empty() {
			atomic.AddInt32(&fw.onGoingRequests, 1)

			checkErr(p.Invoke([3]interface{}{foldChan, fileChan,
				fw.folderQueue.Dequeue()}))

		} else {
			time.Sleep(10 * time.Millisecond) // lighten load for CPU
		}

		workDone = fw.folderQueue.Empty() &&
			atomic.LoadInt32(&fw.onGoingRequests) == 0

	}

	fw.writeWait.Wait()
	fw.progressChan <- &Progress{Files: int(fw.filecount), Folders: int(fw.foldcount),
		Done: true}

}

func (fw *LocalClient) recursiveFoldsearch(args interface{}) {
	unpackArgs := args.([3]interface{})
	writeFold := unpackArgs[0].(chan *File)
	writeFile := unpackArgs[1].(chan *File)
	_ = writeFile
	_ = writeFold
	folderRel, ok := unpackArgs[2].(string)
	if !ok {
		atomic.AddInt32(&fw.onGoingRequests, -1)
		return
	}
	folderAbs := filepath.Join(fw.rootDir, folderRel)
	folders, err := ioutil.ReadDir(folderAbs)
	checkErr(err)
	if err != nil {
		return
	}
	for _, fol := range folders {
		dirpath := filepath.Join(folderRel, fol.Name())
		if fol.IsDir() {

			fw.folderQueue.Enqueue(dirpath)
			writeFold <- fw.hashsum(dirpath, fol)
			atomic.AddInt32(&fw.foldcount, 1)
		} else {
			writeFile <- fw.hashsum(dirpath, fol)
			atomic.AddInt32(&fw.filecount, 1)
		}
	}

	atomic.AddInt32(&fw.onGoingRequests, -1)
}

func (fw *LocalClient) writeFiles(filename string, outchan chan *File) {
	foldpath := filepath.Join(fw.rootDir, ".GoDrive", "local")
	errMk := os.MkdirAll(foldpath, 0777)
	checkErr(errMk)

	file, err := os.Create(filepath.Join(foldpath, filename))
	checkErr(err)
	writer := bufio.NewWriter(file)
	defer file.Close()

	_, err1 := writer.WriteString("[")
	checkErr(err1)
	var i *File
	var ok bool = true
	i, ok = <-outchan
	for fw.canRun && ok {

		if i != nil {
			mr, marErr := json.Marshal(i)
			_, writeErr := writer.WriteString(string(mr))
			checkErr(marErr)
			checkErr(writeErr)
		}
		i, ok = <-outchan
		if ok {
			e, err := writer.WriteString(",\n")
			_ = e
			checkErr(err)
		}

	}

	_, err2 := writer.WriteString("]")
	checkErr(err2)
	err3 := writer.Flush()
	checkErr(err3)
	fw.writeWait.Done()

}

func (fw *LocalClient) createFolderStructure() {
	foldpath := filepath.Join(fw.rootDir, ".GoDrive", "local", "folders.json")
	_ = foldpath
}
