package watcher

import (
	"godrive/internal/googleclient"
	"google.golang.org/api/drive/v3"
	"time"
)

const (
	listFields = "nextPageToken, newStartPageToken, changes(changeType, time," +
		" removed, file(id, name, mimeType, modifiedTime, md5Checksum, parents))"
)

// DriveWatcher watcher struct
type DriveWatcher struct {
	lastSync           time.Time
	lastStartPageToken string
	userID             int
	service            *drive.Service
}

// RegDriveWatcher returns a new watcher for drive
func RegDriveWatcher(id int) (*DriveWatcher, error) {
	dd := new(DriveWatcher)
	dd.userID = id
	dd.lastSync = time.Now()
	var err error
	dd.service, err = googleclient.NewService(id)
	if err != nil {
		return nil, err
	}
	startToken, errT := dd.service.Changes.GetStartPageToken().Do()
	dd.lastStartPageToken = startToken.StartPageToken
	if errT != nil {
		return nil, errT
	}
	return dd, nil
}

// GetChanges gets changes since the last call to GetChanges or StartWatch
func (dw *DriveWatcher) GetChanges() ([]*drive.Change, error) {

	var changeList []*drive.Change = make([]*drive.Change, 0, 1000)
	response, err := dw.service.Changes.List(dw.lastStartPageToken).PageSize(1000).Spaces("drive").
		RestrictToMyDrive(true).Fields(listFields).Do()
	if err != nil {
		return nil, err
	}
	for _, i := range response.Changes {
		changeList = append(changeList, i)
	}

	var nextPage string = response.NextPageToken
	for nextPage != "" {

		response, err = dw.service.Changes.List(nextPage).PageSize(1000).Spaces("drive").
			RestrictToMyDrive(true).Fields(listFields).Do()
		if err != nil {
			return nil, err
		}
		for _, i := range response.Changes {
			changeList = append(changeList, i)
		}
		nextPage = response.NextPageToken

	}

	dw.lastStartPageToken = response.NewStartPageToken
	return changeList, nil
}
