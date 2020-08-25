package localfs

import (
	"encoding/json"
	"errors"
	"godrive/internal/settings"
	"os"
	"path/filepath"
	"sync"
)

// LCStore is the struct to store state
type LCStore struct {
	accessMux    sync.Mutex
	accessID     int
	accessCond   *sync.Cond
	localRoot    string
	driveFileMap map[string]*FileHolder
	driveFoldMap map[string]*FoldHolder
	pathMap      map[string]string
	id           int
	userID       string
	isSaving     bool
}

// AccessLock is the handle to access the locked resource
type AccessLock struct {
	id int
	gs *LCStore
}

// StoreWrite can write and read
type StoreWrite interface {
	ReadFile(string, bool) (*FileHolder, error)
	ReadFold(string, bool) (*FoldHolder, error)
	ReadIDMap(string, bool) (string, error)
	WriteFile(string, *FileHolder, bool) error
	WriteFold(string, *FoldHolder, bool) error
	WriteIDMap(string, string, bool) error
	DeleteFile(string, bool) error
	DeleteFold(string, bool) error
	DeleteIDMap(string, bool) error
	Release() error
}

// StoreRead can only read
type StoreRead interface {
	ReadFile(string, bool) (*FileHolder, error)
	ReadFold(string, bool) (*FoldHolder, error)
	ReadIDMap(string, bool) (string, error)
	Release() error
}

var (
	// ErrNotFound key not found error
	ErrNotFound = errors.New("drive status map key not found")
	// ErrInUse the resource is being used
	ErrInUse = errors.New("drive status resource is being used")
	// ErrAlRelease resource is already freed
	ErrAlRelease = errors.New("resource is already released")
	// ErrInvaID invalid id to unlock resource
	ErrInvaID = errors.New("invalid id to unlock resource")
)

var (
	// drive states for different users
	drivestore map[string]*LCStore = make(map[string]*LCStore)
)

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

// NewStore new drive state storage
func NewStore(id string) (*LCStore, error) {
	set, err := settings.ReadDriveConfig()
	if err != nil {
		return nil, err
	}
	local, err := set.GetUser(id)
	if err != nil {
		return nil, err
	}

	gs, ok := drivestore[id]
	if !ok {
		gs = new(LCStore)
		gs.driveFileMap = make(map[string]*FileHolder, 10000)
		gs.driveFoldMap = make(map[string]*FoldHolder, 10000)
		gs.pathMap = make(map[string]string, 10000)
		gs.id = 0
		gs.accessID = -1
		gs.accessCond = sync.NewCond(&gs.accessMux)
		drivestore[id] = gs
	}

	gs.localRoot = local.LocalRoot
	gs.userID = id

	return gs, nil
}

// ReadFile reads fileMap
// args: (fileID: id of file; blocking: if set true,
// returns ErrInuse if resource is being used, and
// returns ErrNotFound if no such element)
func (al *AccessLock) ReadFile(fileID string, blocking bool) (*FileHolder, error) {
	al.gs.accessCond.L.Lock()
	for al.gs.accessID != al.id {
		if !blocking {
			al.gs.accessCond.L.Unlock()
			return nil, ErrInUse
		}
		al.gs.accessCond.Wait()
	}

	ss, ok := al.gs.driveFileMap[fileID]
	if !ok {
		al.gs.accessCond.L.Unlock()
		return nil, ErrNotFound
	}
	sss := new(FileHolder)
	*sss = *ss
	al.gs.accessCond.L.Unlock()
	return sss, nil

}

// WriteFile writes fileMap
// args: (fileID: id of file; blocking: if set true,
// returns ErrInuse if resource is being used)
func (al *AccessLock) WriteFile(fileID string, fh *FileHolder, blocking bool) error {
	al.gs.accessCond.L.Lock()
	for al.gs.accessID != al.id {
		if !blocking {
			al.gs.accessCond.L.Unlock()
			return ErrInUse
		}
		al.gs.accessCond.Wait()

	}
	ss := new(FileHolder)
	*ss = *fh
	al.gs.driveFileMap[fileID] = ss
	al.gs.accessCond.L.Unlock()
	return nil

}

// DeleteFile deletes entry in filemap with fileID
func (al *AccessLock) DeleteFile(fileID string, blocking bool) error {
	al.gs.accessCond.L.Lock()
	for al.gs.accessID != al.id {
		if !blocking {
			al.gs.accessCond.L.Unlock()
			return ErrInUse
		}
		al.gs.accessCond.Wait()

	}
	delete(al.gs.driveFileMap, fileID)
	al.gs.accessCond.L.Unlock()
	return nil
}

// ReadFold reads foldMap
// args: (fileID: id of file; blocking: if set true,
// returns ErrInuse if resource is being used, and
// returns ErrNotFound if no such element)
func (al *AccessLock) ReadFold(fileID string, blocking bool) (*FoldHolder, error) {
	al.gs.accessCond.L.Lock()
	for al.gs.accessID != al.id {
		if !blocking {
			al.gs.accessCond.L.Unlock()
			return nil, ErrInUse
		}
		al.gs.accessCond.Wait()
	}

	ss, ok := al.gs.driveFoldMap[fileID]
	if !ok {
		al.gs.accessCond.L.Unlock()
		return nil, ErrNotFound
	}
	sss := new(FoldHolder)
	*sss = *ss
	al.gs.accessCond.L.Unlock()
	return sss, nil

}

// WriteFold writes foldMap
// args: (fileID: id of file; blocking: if set true,
// returns ErrInuse if resource is being used)
func (al *AccessLock) WriteFold(fileID string, fh *FoldHolder, blocking bool) error {
	al.gs.accessCond.L.Lock()
	for al.gs.accessID != al.id {
		if !blocking {
			al.gs.accessCond.L.Unlock()
			return ErrInUse
		}
		al.gs.accessCond.Wait()
	}
	ss := new(FoldHolder)
	*ss = *fh
	al.gs.driveFoldMap[fileID] = ss
	al.gs.accessCond.L.Unlock()
	return nil

}

// DeleteFold deletes entry in foldMap with fileID
func (al *AccessLock) DeleteFold(fileID string, blocking bool) error {
	al.gs.accessCond.L.Lock()
	for al.gs.accessID != al.id {
		if !blocking {
			al.gs.accessCond.L.Unlock()
			return ErrInUse
		}
		al.gs.accessCond.Wait()
	}
	delete(al.gs.driveFoldMap, fileID)
	al.gs.accessCond.L.Unlock()
	return nil
}

// ReadIDMap reads pathMap
// args: (fileID: id of file; blocking: if set true,
// returns ErrInuse if resource is being used, and
// returns ErrNotFound if no such element)
func (al *AccessLock) ReadIDMap(path string, blocking bool) (string, error) {
	al.gs.accessCond.L.Lock()
	for al.gs.accessID != al.id {
		if !blocking {
			al.gs.accessCond.L.Unlock()
			return "", ErrInUse
		}
		al.gs.accessCond.Wait()
	}

	ss, ok := al.gs.pathMap[path]
	if !ok {
		al.gs.accessCond.L.Unlock()
		return "", ErrNotFound
	}

	al.gs.accessCond.L.Unlock()
	return ss, nil

}

// WriteIDMap writes pathMap
// args: (fileID: id of file; blocking: if set true,
// returns ErrInuse if resource is being used)
func (al *AccessLock) WriteIDMap(path string, st string, blocking bool) error {
	al.gs.accessCond.L.Lock()
	for al.gs.accessID != al.id {
		if !blocking {
			al.gs.accessCond.L.Unlock()
			return ErrInUse
		}
		al.gs.accessCond.Wait()
	}

	al.gs.pathMap[path] = st
	al.gs.accessCond.L.Unlock()
	return nil

}

// DeleteIDMap deletes entry in pathMap with fileID
func (al *AccessLock) DeleteIDMap(path string, blocking bool) error {
	al.gs.accessCond.L.Lock()
	for al.gs.accessID != al.id {
		if !blocking {
			al.gs.accessCond.L.Unlock()
			return ErrInUse
		}
		al.gs.accessCond.Wait()
	}
	delete(al.gs.pathMap, path)
	al.gs.accessCond.L.Unlock()
	return nil
}

// AcquireWrite acquires write to the resource specified
// by "resource" returns (id, error). This is used to indicate
// the drive state is under heavy modification
func (gs *LCStore) AcquireWrite() (StoreWrite, error) {

	gs.accessCond.L.Lock()
	defer gs.accessCond.L.Unlock()
	if gs.accessID == -1 {
		al := new(AccessLock)
		al.id = gs.getNewID()
		al.gs = gs
		gs.accessID = al.id
		return al, nil
	}
	return nil, ErrInUse

}

// AcquireRead returns the handle to the "resource"
// if the resource is not acquired to be written.
func (gs *LCStore) AcquireRead() (StoreRead, error) {
	al := new(AccessLock)
	al.id = -1
	al.gs = gs
	return al, nil

}

// IsLocked checks whether "resource" is currently
// being accessed. Highly inaccurate.
func (gs *LCStore) IsLocked() bool {

	gs.accessCond.L.Lock()
	defer gs.accessCond.L.Unlock()
	return gs.accessID != -1

}

// Release the hold on the resource acquired
func (al *AccessLock) Release() error {
	al.gs.accessCond.L.Lock()
	defer al.gs.accessCond.L.Unlock()
	if al.gs.accessID != -1 && al.id != -1 {

		if al.gs.accessID == al.id {
			al.gs.accessID = -1
			al.gs.accessCond.Broadcast()
			return nil
		}
		return ErrInvaID
	}
	if al.id == -1 {
		return nil
	}
	return ErrAlRelease

}

func (gs *LCStore) getNewID() int {
	var a = gs.id
	if a > 1000000 {
		gs.id = 0
	} else {
		gs.id++
	}
	return a
}

func (gs *LCStore) writeFiles(filename string) {

	foldpath := filepath.Join(gs.localRoot, ".GoDrive", "remote")
	errMk := os.MkdirAll(foldpath, 0777)
	checkErr(errMk)

	file, err := os.Create(filepath.Join(foldpath, filename))
	checkErr(err)
	defer file.Close()
	err = json.NewEncoder(file).Encode(gs.driveFileMap)
	checkErr(err)

}

func (gs *LCStore) writeFolds(foldList string, foldIDmap string) {

	foldpath := filepath.Join(gs.localRoot, ".GoDrive", "remote")
	errMk := os.MkdirAll(foldpath, 0777)
	checkErr(errMk)

	list, err1 := os.Create(filepath.Join(foldpath, foldList))
	checkErr(err1)
	defer list.Close()
	err1 = json.NewEncoder(list).Encode(gs.driveFoldMap)
	checkErr(err1)

	Ids, err2 := os.Create(filepath.Join(foldpath, foldIDmap))
	checkErr(err2)
	defer Ids.Close()
	err2 = json.NewEncoder(Ids).Encode(gs.pathMap)
	checkErr(err2)

}

// Save the current drive state to the files as (foldList, fileList, foldIDMap)
func (gs *LCStore) Save(foldList string, fileList string, foldIDMap string) {
	if gs.isSaving {
		return
	}
	gs.isSaving = true
	go func() {
		defer func() {
			gs.isSaving = false
		}()
		gs.writeFiles(fileList)
		gs.writeFolds(foldList, foldIDMap)

	}()
}
