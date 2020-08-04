package main

import (
	"errors"
	"fmt"
	"godrive/internal/gdrive"
	"godrive/internal/googleclient"
	"godrive/internal/localfs"
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
	gclient, err := gdrive.NewClient("/home/kie/test", "root")
	handleGDriveError(err)
	if err != nil {
		log.Fatalf("Undefined error: %v", err)

	}

	sd := gclient.ListAll()

	select {
	case r := <-sd:
		r1, r2 := r.Folders, r.Files
		fmt.Printf("folders: %d files: %d\n", r1, r2)

		elapsed1 := time.Now().Sub(begin1).Seconds()
		fmt.Printf("time spent: %f s\n", elapsed1)
	}
}

func localSync() {
	begin2 := time.Now()
	fw1 := localfs.NewWatcher("/home/kie/test")
	fw2 := fw1.ListAll()

	select {
	case r := <-fw2:
		r1, r2, err := r.Folders, r.Files, r.Error
		fmt.Printf("folders: %d files: %d error: %v\n", r1, r2, err)
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
	d, err := watcher.RegDriveWatcher(0)

	if err == nil {
		for {
			go getChange(d)
			time.Sleep(3 * time.Second)
		}
	}
}

func main() {
	// watchChanges()
	remoteSync()
}
