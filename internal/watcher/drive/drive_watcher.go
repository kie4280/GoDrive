package gdwatch

import (
	"godrive/internal/googleclient"
	"google.golang.org/api/drive/v3"
	"time"
)

const (
	listFields = "nextPageToken, newStartPageToken, changes(changeType, time, " +
		"removed, fileId, file(name, mimeType, modifiedTime, md5Checksum, parents))"
)

// DriveWatcher watcher struct
type DriveWatcher struct {
	lastSync           time.Time
	refreshInterv      int
	lastStartPageToken string
	userID             string
	service            *drive.Service
	globalError        error // The error to return when GetDriveChanges is called
	canRun             bool
	isRunning          bool
	changeList         []*drive.Change
}

// RegDriveWatcher returns a new watcher for drive (id: userID)
func RegDriveWatcher(id string) (*DriveWatcher, error) {
	dd := new(DriveWatcher)
	dd.userID = id
	dd.lastSync = time.Now()
	dd.refreshInterv = 6
	var err error
	dd.service, err = googleclient.NewService(id)
	dd.changeList = make([]*drive.Change, 0, remoteChangeListSize)
	if err != nil {
		return nil, err
	}
	go dd.startWatcher()
	return dd, nil
}

func (dw *DriveWatcher) onDriveError() {
	if err := recover(); err != nil {
		err1 := err.(error)
		dw.globalError = err1
	}
}

func (dw *DriveWatcher) startWatcher() {
	defer dw.onDriveError()
	defer func() {
		dw.isRunning = false
		dw.canRun = false
	}()
	startToken, errT := dw.service.Changes.GetStartPageToken().Do()
	checkErr(errT)
	dw.lastStartPageToken = startToken.StartPageToken
	dw.isRunning = true
	dw.canRun = true
	for dw.canRun {
		if time.Now().Sub(dw.lastSync).Seconds() < float64(dw.refreshInterv) {
			time.Sleep(1000)
			continue
		}

		response, err := dw.service.Changes.List(dw.lastStartPageToken).
			PageSize(1000).Spaces("drive").RestrictToMyDrive(true).
			Fields(listFields).Do()
		checkErr(err)
		for _, i := range response.Changes {
			dw.changeList = append(dw.changeList, i)
		}

		var nextPage string = response.NextPageToken
		for nextPage != "" {

			response, err = dw.service.Changes.List(nextPage).PageSize(1000).
				Spaces("drive").RestrictToMyDrive(true).Fields(listFields).Do()
			checkErr(err)
			for _, i := range response.Changes {
				dw.changeList = append(dw.changeList, i)
			}
			nextPage = response.NextPageToken

		}

		dw.lastStartPageToken = response.NewStartPageToken
		dw.lastSync = time.Now()
	}

}

// GetDriveChanges gets changes since the last call to GetDriveChanges
func (dw *DriveWatcher) GetDriveChanges() ([]*drive.Change, error) {
	if dw.globalError != nil {
		return nil, dw.globalError
	}
	changes := make([]*drive.Change, len(dw.changeList))
	copy(changes, dw.changeList)
	dw.changeList = make([]*drive.Change, 0, remoteChangeListSize)
	return changes, nil
}

// IsRunning returns true if DriveWatcher is watching
func (dw *DriveWatcher) IsRunning() bool {
	return dw.isRunning
}

// Close all resources
func (dw *DriveWatcher) Close() {
	dw.canRun = false
}
