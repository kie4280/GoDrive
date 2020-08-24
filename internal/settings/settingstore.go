package settings

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
)

// DriveConfigs contains all the possible settings
type DriveConfigs struct {
	global *GlobalConfig
}

// GlobalConfig stores to json in this format
type GlobalConfig struct {
	Usercount  int
	AccountIDs []string
	Users      map[string]*UserConfigs
}

// UserConfigs contains the setting of a particular user
type UserConfigs struct {
	AccountName string
	LocalRoot   string
}

var driveconfig *DriveConfigs = nil

var (
	// ErrNoSuchUser means the user is deleted
	ErrNoSuchUser = errors.New("The user is deleted")
)

// ReadDriveConfig reads the google drive configs from the config file
func ReadDriveConfig() (*DriveConfigs, error) {

	if driveconfig != nil {
		return driveconfig, nil
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
			config := new(GlobalConfig)
			config.Usercount = 0
			config.Users = make(map[string]*UserConfigs)
			driveconfig = &DriveConfigs{global: config}
			return driveconfig, nil
		}

		return nil, err
	}
	config := new(GlobalConfig)
	err = json.NewDecoder(file).Decode(config)
	out := new(DriveConfigs)
	out.global = config
	driveconfig = out
	return out, err
}

// SaveDriveConfig saves the configuration of a user to file
func SaveDriveConfig() error {
	homedir, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	configPath := filepath.Join(homedir, ".GoDrive", "driveconfig.json")
	os.MkdirAll(filepath.Join(homedir, ".GoDrive"), 0777)
	file, err := os.Create(configPath)
	defer file.Close()
	err = json.NewEncoder(file).Encode(driveconfig.global)
	return err
}

// ListIDs the user names
func (dc *DriveConfigs) ListIDs() []string {
	return dc.global.AccountIDs
}

// GetUser the user with "id"
func (dc *DriveConfigs) GetUser(id string) (*UserConfigs, error) {
	ur, ok := dc.global.Users[id]
	if !ok {
		return nil, ErrNoSuchUser
	}
	return ur, nil
}

// Add user to global config and return the user Id
func (dc *DriveConfigs) Add(user *UserConfigs) string {
	id := GetID(user.AccountName)
	_, ok := dc.global.Users[id]
	if !ok {
		dc.global.Usercount++
		dc.global.AccountIDs = append(dc.global.AccountIDs, id)
	}

	dc.global.Users[id] = user
	return id
}

// GetID gets the id of a particular account name
func GetID(account string) string {
	hash := md5.New()
	io.WriteString(hash, account)
	id := hex.EncodeToString(hash.Sum(nil))
	return id
}
