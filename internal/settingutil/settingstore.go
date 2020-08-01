package settingutil

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// DriveConfigs contains all the possible settings
type DriveConfigs struct {
	usercount int
	users     []*UserConfigs
}

// UserConfigs contains the setting of a particular user
type UserConfigs struct {
	account     string
	driveRootID string
	syncDest    string
}

// ReadDriveConfig reads the google drive configs from the config file
func ReadDriveConfig() (*DriveConfigs, error) {
	homedir, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	configPath := filepath.Join(homedir, ".GoDrive", "driveconfig.json")
	file, err := os.Open(configPath)
	defer file.Close()
	if err != nil {
		if os.IsNotExist(err) {
			return &DriveConfigs{usercount: 0, users: nil}, nil
		}

		return nil, err
	}
	config := new(DriveConfigs)
	err = json.NewDecoder(file).Decode(config)
	return config, err
}

// SaveDriveConfig saves the configuration of a user to file
func SaveDriveConfig(configs *DriveConfigs) error {
	homedir, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	configPath := filepath.Join(homedir, ".GoDrive", "driveconfig.json")
	file, err := os.Create(configPath)
	defer file.Close()
	err = json.NewEncoder(file).Encode(configs)
	return err
}
