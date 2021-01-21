package settings

import (
	"encoding/json"
	"errors"
	"godrive/internal/utils"
	"os"
	"path/filepath"
	"sync"
)

/*
This setting store is safe for concurrent reads and writes.
This is where the settings for the whole program is stored.

*/

// DriveConfigHandle is the handle to all the possible settings
type DriveConfigHandle interface {
	Add(string, string) string
	ListIDs() []string
	GetUser(string) (UserConfigHandle, error)
	Delete(string)
}

// UserConfigHandle is the interface returned by GetUser
type UserConfigHandle interface {
	GetAccountName() string
	GetSyncRoot() string
	IsIgnored(string) bool
}

// Global stores to json in this format
type Global struct {
	Usercount  int
	AccountIDs []string
	Users      map[string]*User
	globalLock sync.Mutex
}

// User contains the setting of a particular user
type User struct {
	AccountName string
	syncDir     string
	Excluded    []string
	userLock    sync.Mutex
}

var globalConfig *Global = nil
var fileLock sync.Mutex

var (
	// ErrNoSuchUser means the user is deleted
	ErrNoSuchUser = errors.New("The user is deleted")
)

// ReadDriveConfig reads the google drive configs from the config file
func ReadDriveConfig() (DriveConfigHandle, error) {
	fileLock.Lock()
	defer fileLock.Unlock()
	if globalConfig != nil {
		return globalConfig, nil
	}
	homedir, err := os.UserHomeDir()

	if err != nil {
		panic(err)
	}
	configPath := filepath.Join(homedir, ".GoDrive", "driveconfig.json")
	file, err := os.Open(configPath)
	defer file.Close()
	if err != nil {
		if os.IsNotExist(err) {
			config := new(Global)
			config.Usercount = 0
			config.Users = make(map[string]*User)
			globalConfig = config
			return config, nil
		}

		return nil, err
	}
	config := new(Global)
	err = json.NewDecoder(file).Decode(config)
	globalConfig = config
	return config, err
}

// SaveDriveConfig saves the configuration of a user to file
func SaveDriveConfig() error {
	fileLock.Lock()
	defer fileLock.Unlock()
	homedir, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	configPath := filepath.Join(homedir, ".GoDrive", "driveconfig.json")
	err = os.MkdirAll(filepath.Join(homedir, ".GoDrive"), 0777)
	if err != nil {
		return err
	}
	file, err := os.Create(configPath)
	if err != nil {
		return err
	}
	defer file.Close()
	err = json.NewEncoder(file).Encode(globalConfig)
	globalConfig = nil
	return err
}

// ListIDs the user names
func (gc *Global) ListIDs() []string {
	return gc.AccountIDs
}

// GetUser the user with "id"
func (gc *Global) GetUser(id string) (UserConfigHandle, error) {
	ur, ok := gc.Users[id]
	if !ok {
		return nil, ErrNoSuchUser
	}
	return ur, nil
}

// Add user to global config and return the user Id
func (gc *Global) Add(account string, localRoot string) string {
	id := utils.StringToMd5(account)
	gc.globalLock.Lock()
	defer gc.globalLock.Unlock()
	_, ok := gc.Users[id]
	if !ok {
		gc.Usercount++
		gc.AccountIDs = append(gc.AccountIDs, id)
		cc := new(User)
		cc.AccountName = account
		cc.syncDir = filepath.Clean(localRoot)
		gc.Users[id] = cc
	}

	return id
}

// Delete a user
func (gc *Global) Delete(id string) {
	gc.globalLock.Lock()
	defer gc.globalLock.Unlock()
	delete(gc.Users, id)
	for i, a := range gc.AccountIDs {
		if a == id {
			gc.AccountIDs = append(gc.AccountIDs[0:i],
				gc.AccountIDs[i+i:]...)
			break
		}
	}
	gc.Usercount--
}

// GetAccountName gets account name
func (uc *User) GetAccountName() string {
	return uc.AccountName
}

// GetSyncRoot gets local root
func (uc *User) GetSyncRoot() string {
	return uc.syncDir
}

// IsIgnored returns whether the file or folder is ignored
func (uc *User) IsIgnored(path string) bool {
	return false
}
