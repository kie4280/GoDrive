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
	fw1, err := localfs.NewClient(userID)
	log.Println(err)
	fw2 := fw1.ListAll()
loop:
	for {
		select {
		case err := <-fw2.Error():
			fmt.Println(err)
			break loop
		default:
		}
		select {
		case r := <-fw2.Progress():
			r1, r2 := r.Folders, r.Files
			fmt.Printf("folders: %d files: %d\n", r1, r2)
			if r.Done {
				break loop
			}
		}
	}

	elapsed2 := time.Now().Sub(begin2).Seconds()
	fmt.Printf("time spent: %f s\n", elapsed2)

}

func getChange(d *watcher.DriveWatcher) {
	changes, e := d.GetDriveChanges()

	if e == nil {
		for _, i := range changes {
			if !i.Removed {
				fmt.Printf("change: %v %v %v %v %v\n", i.Time,
					i.File.Name, i.FileId, i.File.Parents,
					i.ChangeType)
			} else {
				fmt.Printf("change: %v %v %v\n", i.Time,
					i.ChangeType, i.FileId)
			}

		}
	} else {
		log.Fatalf("getchange error: %v\n", e)
	}
}

func watchRemote() {
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

func watchLocal() {
	lw, err := watcher.RegfsWatcher(userID)
	if err != nil {
		log.Fatalln(err)
	}
	defer lw.Close()
	time.Sleep(1000 * time.Second)
}

var userID string

func main() {
	config, err := settings.ReadDriveConfig()
	fmt.Printf("setting error: %v\n", err)
	userID = config.Add("duckfat0000@gmail.com",
		"/home/kie/test")
	defer settings.SaveDriveConfig()

	// watchRemote()
	// remoteSync()
	// download()
	// mkdir()
	watchLocal()
	// localSync()

}
