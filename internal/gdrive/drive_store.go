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

const (
	// R_FILEMAP filemap resource
	R_FILEMAP int = 0
	// R_FOLDMAP foldmap resource
	R_FOLDMAP int = 1
	// R_PATHMAP pathmap resource
	R_PATHMAP int = 2
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
func (gs *GDStore) AccessFile(fileID string, checkExist bool) (*FileHolder, error) {

	gs.fileMapMux.Lock()
	defer gs.fileMapMux.Unlock()
	if checkExist {
		ss, ok := gs.driveFileMap[fileID]
		if !ok {
			return nil, ErrNotFound
		}
		return ss, nil
	}
	gs.driveFileMap[fileID] = new(FileHolder)
	return gs.driveFileMap[fileID], nil

}

// DeleteFile deletes entry in filemap with fileID
func (gs *GDStore) DeleteFile(fileID string) {
	gs.fileMapMux.Lock()
	defer gs.fileMapMux.Unlock()
	delete(gs.driveFileMap, fileID)
}

// AccessFold allows access to foldMap
// args: (fileID: id of file, checkExist: if set true,
// returns ErrNotFound if no such element)
func (gs *GDStore) AccessFold(fileID string, checkExist bool) (*FoldHolder, error) {
	gs.foldMapMux.Lock()
	defer gs.foldMapMux.Unlock()
	if checkExist {
		ss, ok := gs.driveFoldMap[fileID]
		if !ok {
			return nil, ErrNotFound
		}
		return ss, nil
	}
	gs.driveFoldMap[fileID] = new(FoldHolder)
	return gs.driveFoldMap[fileID], nil
}

// DeleteFold deletes the entry in foldmap with fileID
func (gs *GDStore) DeleteFold(fileID string) {
	gs.foldMapMux.Lock()
	defer gs.foldMapMux.Unlock()
	delete(gs.driveFoldMap, fileID)
}

// AccessIDMap allows access to IDMap
// args: (fileID: id of file, write: if set true,
// returns ErrNotFound if no such element, content: id input)
func (gs *GDStore) AccessIDMap(path string, write bool, content *string) error {
	gs.pathMapMux.Lock()
	defer gs.pathMapMux.Unlock()
	if write {
		gs.pathMap[path] = *content
	} else {

		ss, ok := gs.pathMap[path]
		if !ok {
			return ErrNotFound
		}
		*content = ss
	}

	return nil
}

//DeleteIDMap deletes the entry in IDMap with fileID
func (gs *GDStore) DeleteIDMap(fileID string) {
	gs.pathMapMux.Lock()
	defer gs.pathMapMux.Unlock()
	delete(gs.pathMap, fileID)
}

// Acquire the resource specified by "resource"
// returns (id, error)
func (gs *GDStore) Acquire(resource int) (int, error) {
	gs.updateMux.Lock()
	defer gs.updateMux.Unlock()
	switch resource {
	case R_FILEMAP:
		if gs.updating[R_FILEMAP] == -1 {
			a := gs.getNewID()
			gs.updating[R_FILEMAP] = a
			return a, nil
		}
		return -1, ErrInUse
	case R_FOLDMAP:
		if gs.updating[R_FOLDMAP] == -1 {
			a := gs.getNewID()
			gs.updating[R_FOLDMAP] = a
			return a, nil
		}
		return -1, ErrInUse
	case R_PATHMAP:
		if gs.updating[R_PATHMAP] == -1 {
			a := gs.getNewID()
			gs.updating[R_PATHMAP] = a
			return a, nil
		}
		return -1, ErrInUse
	default:
		panic(errors.New("undefined resource"))
	}
}

// IsLocked checks whether "resource" is currently being accessed
func (gs *GDStore) IsLocked(resource int) bool {
	gs.updateMux.Lock()
	defer gs.updateMux.Unlock()
	if gs.updating[resource] == -1 {
		return false
	}
	return true
}

// Release the hold on "resource"
func (gs *GDStore) Release(resource int, id int) error {
	gs.updateMux.Lock()
	defer gs.updateMux.Unlock()
	if gs.updating[resource] != -1 {

		if gs.updating[resource] == id {
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
