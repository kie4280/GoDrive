package gdrive

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
)

// GDStore is the struct to store state
type GDStore struct {
	fileMapMux sync.Mutex
	foldMapMux sync.Mutex
	pathMapMux sync.Mutex
	updateMux  sync.Mutex

	localRoot    string
	remoteRootID string
	driveFileMap map[string]*FileHolder
	driveFoldMap map[string]*FoldHolder
	pathMap      map[string]string
	updating     [3]int
	id           int
}

// AccessLock is the handle to access the locked resource
type AccessLock struct {
	resource int8
	id       int
	gs       *GDStore
}

const (
	// R_FILEMAP filemap resource
	R_FILEMAP int8 = 0
	// R_FOLDMAP foldmap resource
	R_FOLDMAP int8 = 1
	// R_PATHMAP pathmap resource
	R_PATHMAP int8 = 2
	//

)

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

// NewStore new drive state storage
func NewStore(localDir string, remoteID string) *GDStore {
	gs := new(GDStore)
	gs.localRoot = localDir
	gs.remoteRootID = remoteID
	gs.driveFileMap = make(map[string]*FileHolder, 10000)
	gs.driveFoldMap = make(map[string]*FoldHolder, 10000)
	gs.pathMap = make(map[string]string, 10000)
	gs.id = 0
	gs.updating = [3]int{-1, -1, -1}
	return gs
}

// AccessFile allows access to fileMap
// args: (fileID: id of file, checkExist: if set true,
// returns ErrNotFound if no such element)
func (al *AccessLock) AccessFile(fileID string, checkExist bool) (*FileHolder, error) {
	if al.gs.updating[al.resource] != al.id {
		return nil, ErrInUse
	}
	al.gs.fileMapMux.Lock()
	defer al.gs.fileMapMux.Unlock()
	if checkExist {
		ss, ok := al.gs.driveFileMap[fileID]
		if !ok {
			return nil, ErrNotFound
		}
		return ss, nil
	}
	al.gs.driveFileMap[fileID] = new(FileHolder)
	return al.gs.driveFileMap[fileID], nil

}

// DeleteFile deletes entry in filemap with fileID
func (al *AccessLock) DeleteFile(fileID string) error {
	if al.gs.updating[al.resource] != al.id {
		return ErrInUse
	}
	al.gs.fileMapMux.Lock()
	defer al.gs.fileMapMux.Unlock()
	delete(al.gs.driveFileMap, fileID)
	return nil
}

// AccessFold allows access to foldMap
// args: (fileID: id of file, checkExist: if set true,
// returns ErrNotFound if no such element)
func (al *AccessLock) AccessFold(fileID string, checkExist bool) (*FoldHolder, error) {
	if al.gs.updating[al.resource] != al.id {
		return nil, ErrInUse
	}
	al.gs.foldMapMux.Lock()
	defer al.gs.foldMapMux.Unlock()
	if checkExist {
		ss, ok := al.gs.driveFoldMap[fileID]
		if !ok {
			return nil, ErrNotFound
		}
		return ss, nil
	}
	al.gs.driveFoldMap[fileID] = new(FoldHolder)
	return al.gs.driveFoldMap[fileID], nil
}

// DeleteFold deletes the entry in foldmap with fileID
func (al *AccessLock) DeleteFold(fileID string) error {
	if al.gs.updating[al.resource] != al.id {
		return ErrInUse
	}
	al.gs.foldMapMux.Lock()
	defer al.gs.foldMapMux.Unlock()
	delete(al.gs.driveFoldMap, fileID)
	return nil
}

// AccessIDMap allows access to IDMap
// args: (fileID: id of file, write: if set true,
// returns ErrNotFound if no such element, content: id input)
func (al *AccessLock) AccessIDMap(path string, write bool, content *string) error {
	if al.gs.updating[al.resource] != al.id {
		return ErrInUse
	}
	al.gs.pathMapMux.Lock()
	defer al.gs.pathMapMux.Unlock()
	if write {
		al.gs.pathMap[path] = *content
	} else {

		ss, ok := al.gs.pathMap[path]
		if !ok {
			return ErrNotFound
		}
		*content = ss
	}

	return nil
}

//DeleteIDMap deletes the entry in IDMap with fileID
func (al *AccessLock) DeleteIDMap(fileID string) error {
	if al.gs.updating[al.resource] != al.id {
		return ErrInUse
	}
	al.gs.pathMapMux.Lock()
	defer al.gs.pathMapMux.Unlock()
	delete(al.gs.pathMap, fileID)
	return nil
}

// Acquire the resource specified by "resource"
// returns (id, error). This is used to indicate
// the drive state is under heavy modification
func (gs *GDStore) Acquire(resource int8) (*AccessLock, error) {
	gs.updateMux.Lock()
	defer gs.updateMux.Unlock()
	a := gs.getNewID()
	switch resource {
	case R_FILEMAP:
		if gs.updating[R_FILEMAP] == -1 {
			gs.updating[R_FILEMAP] = a
			al := new(AccessLock)
			al.resource = R_FILEMAP
			al.id = a
			al.gs = gs
			return al, nil
		}
		return nil, ErrInUse
	case R_FOLDMAP:
		if gs.updating[R_FOLDMAP] == -1 {
			gs.updating[R_FOLDMAP] = a
			al := new(AccessLock)
			al.resource = R_FOLDMAP
			al.id = a
			al.gs = gs
			return al, nil
		}
		return nil, ErrInUse
	case R_PATHMAP:
		if gs.updating[R_PATHMAP] == -1 {
			gs.updating[R_PATHMAP] = a
			al := new(AccessLock)
			al.resource = R_PATHMAP
			al.id = a
			al.gs = gs
			return al, nil
		}
		return nil, ErrInUse
	default:
		panic(errors.New("undefined resource"))
	}
}

// IsLocked checks whether "resource" is currently being accessed
func (gs *GDStore) IsLocked(resource int8) bool {
	gs.updateMux.Lock()
	defer gs.updateMux.Unlock()
	if gs.updating[resource] == -1 {
		return false
	}
	return true
}

// Release the hold on "resource"
func (gs *GDStore) Release(al *AccessLock) error {
	gs.updateMux.Lock()
	defer gs.updateMux.Unlock()
	if gs.updating[al.resource] != -1 {

		if gs.updating[al.resource] == al.id {
			return nil
		}
		return ErrInvaID
	}
	return ErrAlRelease

}

func (gs *GDStore) getNewID() int {
	var a = gs.id
	if a > 1000000 {
		gs.id = 0
	} else {
		gs.id++
	}
	return a
}

func (gs *GDStore) writeFiles(filename string) {

	foldpath := filepath.Join(gs.localRoot, ".GoDrive", "remote")
	errMk := os.MkdirAll(foldpath, 0777)
	checkErr(errMk)

	file, err := os.Create(filepath.Join(foldpath, filename))
	checkErr(err)
	defer file.Close()
	err = json.NewEncoder(file).Encode(gs.driveFileMap)
	checkErr(err)

}

func (gs *GDStore) writeFolds(foldList string, foldIDmap string) {

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
func (gs *GDStore) Save(foldList string, fileList string, foldIDMap string) {
	go func() {
		gs.writeFiles(fileList)
		gs.writeFolds(foldList, foldIDMap)
	}()
}
