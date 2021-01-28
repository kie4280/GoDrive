package localfs

import (
	"database/sql"
	"errors"
	// import driver
	_ "github.com/mattn/go-sqlite3"
	"godrive/internal/settings"
	"path/filepath"
	"sync"
	"time"
)

// LCStore is the struct to store state
type LCStore struct {
	accessCount int
	accessCond  *sync.Cond
	localRoot   string
	sqlConn     *sql.DB
	userID      string
	isSaving    bool
}

// AccessLock is the handle to access the locked resource
type AccessLock struct {
	gs          *LCStore
	tableName   string
	operation   int8
	transaction *sql.Tx
}

// StoreWrite can write and read
type StoreWrite interface {
	// ReadFile(string) (*FileHolder, error)
	// ReadFold(string) (*FoldHolder, error)
	WriteFile(*FileHolder) error
	WriteFold(*FoldHolder) error
	Release() error
}

// StoreRead can only read
type StoreRead interface {
	// ReadFile(string) (*FileHolder, error)
	// ReadFold(string) (*FoldHolder, error)
	Release() error
}

var (
	// ErrTableNotFound table not found error
	ErrTableNotFound = errors.New("sql table not found")
	// ErrInUse the resource is being used
	ErrInUse = errors.New("resource is being used")
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
	Name        string
	MimeType    string
	ModTime     time.Time
	DriveFileID string
	Md5Chk      string
}

// FoldHolder holds a folder
type FoldHolder struct {
	Name        string
	ModTime     time.Time
	DriveFileID string
}

// Store returns the local store for user "id"
func Store(id string) (*LCStore, error) {
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
		var err error
		gs.sqlConn, err = sql.Open("sqlite3", filepath.Join(local.GetSyncRoot(),
			".GoDrive/local/fs_state.db"))
		if err != nil {
			return nil, err
		}

		gs.accessCount = 0
		gs.accessCond = sync.NewCond(&sync.Mutex{})
		drivestore[id] = gs
	}

	gs.localRoot = local.GetSyncRoot()
	gs.userID = id

	return gs, nil
}

func (gs *LCStore) createTable(tableName string) error {
	gs.sqlConn.Ping()
	_, err := gs.sqlConn.Exec(`
	CREATE TABLE ?(
		Path TEXT NOT NULL, 
		Type TEXT NOT NULL,
		ModTime TEXT,
		DriveFileID TEXT,
		MD5 TEXT
	); 
	`, tableName)
	if err != nil && err.Error() != "table "+tableName+"already exists" {
		return err
	}
	return nil
}

func (gs *LCStore) tableExists(tableName string) bool {
	err := gs.sqlConn.Ping()
	checkErr(err)
	_, err = gs.sqlConn.Query("SELECT (Path) FROM ?;", tableName)
	if err.Error() == "no such table" {
		return false
	}
	return true

}

// ReadFile reads fileMap
func (al *AccessLock) ReadFile(hints map[string]string) (*FileHolder, error) {

	return nil, nil

}

// WriteFile writes file to table
func (al *AccessLock) WriteFile(fh *FileHolder) error {
	modTB, err := fh.ModTime.MarshalText()
	checkErr(err)
	modTime := string(modTB)
	_, err = al.transaction.Exec(`
	INSERT INTO ?(Path, Type, ModTime, DriveFileID, MD5)
	VALUES(?, ?, ?, ?, ?);
	`, al.tableName, fh.Name, fh.MimeType, modTime, fh.DriveFileID, fh.Md5Chk)

	return err
}

// ReadFold reads foldMap
func (al *AccessLock) ReadFold(fileID string) (*FoldHolder, error) {

	return nil, nil

}

// WriteFold writes folder to table
func (al *AccessLock) WriteFold(fh *FoldHolder) error {
	modTB, err := fh.ModTime.MarshalText()
	checkErr(err)
	modTime := string(modTB)
	_, err = al.transaction.Exec(`
	INSERT INTO ?(Path, Type, ModTime, DriveFileID, MD5)
	VALUES(?, ?, ?, ?, ?);
	`, al.tableName, fh.Name, "folder", modTime, fh.DriveFileID, "")

	return err
}

// Release the hold on the resource acquired
func (al *AccessLock) Release() error {
	al.gs.accessCond.L.Lock()
	defer al.gs.accessCond.L.Unlock()
	if al.gs.accessCount == -1 && al.operation > 0 {

		al.gs.accessCount += int(al.operation)
		al.operation = 0
		al.gs.accessCond.Broadcast()
		return al.transaction.Commit()
	} else if al.gs.accessCount > 0 && al.operation < 0 {
		al.gs.accessCount += int(al.operation)
		al.operation = 0
		if al.gs.accessCount == 0 {
			al.gs.accessCond.Broadcast()
		}
	} else {
		return ErrAlRelease
	}
	return nil

}

// AcquireWrite acquires write to the resource.
// args: (tableName: name of the table to write,
// blocking: block if set to true, otherwise return ErrInUse
// if drive state is under modification)
func (gs *LCStore) AcquireWrite(tableName string, blocking bool) (StoreWrite, error) {

	gs.accessCond.L.Lock()
	for gs.accessCount != 0 {
		if !blocking {
			gs.accessCond.L.Unlock()
			return nil, ErrInUse
		}
		gs.accessCond.Wait()
	}
	gs.accessCount = -1
	gs.accessCond.L.Unlock()
	err := gs.createTable(tableName)
	checkErr(err)
	al := new(AccessLock)
	al.operation = 1
	al.tableName = tableName
	al.gs = gs
	al.transaction, err = gs.sqlConn.Begin()
	checkErr(err)

	return al, nil

}

// AcquireRead returns the handle to the "resource"
// if the resource is not acquired to be written.
func (gs *LCStore) AcquireRead(tableName string, blocking bool) (StoreRead, error) {
	gs.accessCond.L.Lock()
	for gs.accessCount < 0 {
		if !blocking {
			gs.accessCond.L.Unlock()
			return nil, ErrInUse
		}
		gs.accessCond.Wait()
	}
	gs.accessCount++
	gs.accessCond.L.Unlock()
	if !gs.tableExists(tableName) {
		return nil, ErrTableNotFound
	}
	al := new(AccessLock)
	al.operation = -1
	al.gs = gs
	al.tableName = tableName
	return al, nil

}

// IsLocked checks whether "resource" is currently
// being accessed (Highly inaccurate).
func (gs *LCStore) IsLocked() bool {

	gs.accessCond.L.Lock()
	defer gs.accessCond.L.Unlock()
	return gs.accessCount != 0

}

// Save the current drive state to the files as (foldList, fileList, foldIDMap)
func (gs *LCStore) Save() {
	gs.sqlConn.Close()
}

// Exist the current drive state to the files as (foldList, fileList, foldIDMap)
func (gs *LCStore) Exist() bool {
	gs.accessCond.L.Lock()
	defer gs.accessCond.L.Unlock()
	return false
}
