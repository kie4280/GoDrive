package main

import (
	"errors"
	"fmt"
	"godrive/internal/gdrive"
	"godrive/internal/googleclient"
	"godrive/internal/localfs"
	"godrive/internal/settings"
	"godrive/internal/watcher"
	"log"
	"time"
)

func handleGDriveError(err error) {
	if err != nil {
		if errors.Is(err, googleclient.ErrAuthWebCode) {

		} else if errors.Is(err, googleclient.ErrCacheOauth) {

		} else if errors.Is(err, googleclient.ErrParseError) {

		} else if errors.Is(err, googleclient.ErrReadSecret) {

		} else if errors.Is(err, googleclient.ErrUserAuthCodeError) {

		} else {
			log.Fatalf("Undefined error: %v", err)
		}
	}
}

func remoteSync() {
	begin1 := time.Now()
	gclient, err := gdrive.NewClient(userID)
	handleGDriveError(err)
	if err != nil {
		log.Fatalf("Undefined error: %v", err)

	}

	sd := gclient.ListAll()

	// time.Sleep(4 * time.Second)
	// sd <- &gdrive.ListProgress{Command: gdrive.C_CANCEL}
loop:
	for {

		select {
		case r := <-sd.Error():
			fmt.Printf("Error: %v", fmt.Errorf("%w\n", r))

		default:
		}
		select {
		case r := <-sd.Progress():
			r1, r2 := r.Folders, r.Files
			fmt.Printf("folders: %d files: %d\n", r1, r2)
			if r.Done {
				break loop
			}
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}

	elapsed1 := time.Now().Sub(begin1).Seconds()
	fmt.Printf("time spent: %f s\n", elapsed1)

}

func localSync() {
	begin2 := time.Now()
	fw1 := localfs.NewClient("/home/kie/test")
	fw2 := fw1.ListAll()

	select {
	case r := <-fw2:
		r1, r2 := r.Folders, r.Files
		fmt.Printf("folders: %d files: %d error: \n", r1, r2)
	}
	elapsed2 := time.Now().Sub(begin2).Seconds()
	fmt.Printf("time spent: %f s\n", elapsed2)

}

func getChange(d *watcher.DriveWatcher) {
	changes, e := d.GetDriveChanges()

	if e == nil {
		for _, i := range changes {
			fmt.Printf("change: %v %v %v %v\n", i.Time, i.File.Name, i.File.Id, i.File.Parents)
		}
	} else {
		fmt.Printf("getchange error: %v\n", e)
	}
}

func watchChanges() {
	d, err := watcher.RegDriveWatcher(userID)

	if err == nil {
		for {
			getChange(d)
			time.Sleep(3 * time.Second)
		}
	}
}

func download() {
	a, err := gdrive.NewClient(userID)
	_ = err
	dh := a.Download("1Qx2tb7_HbxeLEHvmG0ECvbmrRz0-ky9d", "/")
	_ = dh
}

func mkdir() {
	a, err := gdrive.NewClient(userID)
	_ = err
	a.MkdirAll("/hello")
	time.Sleep(10 * time.Second)
}

var userID string

func main() {
	config, err := settings.ReadDriveConfig()
	fmt.Printf("error: %v\n", err)
	userID = config.Add(&settings.UserConfigs{
		AccountName: "duckfat0000@gmail.com",
		LocalRoot:   "/home/kie/test"})
	defer settings.SaveDriveConfig()

	// watchChanges()
	remoteSync()
	// download()
	// mkdir()

}
