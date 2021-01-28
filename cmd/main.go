package main

import (
	"errors"

	"godrive/internal/gdrive"
	"godrive/internal/googleclient"
	"godrive/internal/localfs"
	"godrive/internal/settings"
	"godrive/internal/watcher"
	"log"
	"net/http"
	_ "net/http/pprof"
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
			log.Printf("Error: %v", r)

		default:
		}
		select {
		case r := <-sd.Progress():
			r1, r2 := r.Folders, r.Files
			log.Printf("folders: %d files: %d\n", r1, r2)
			if r.Done {
				break loop
			}
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}

	elapsed1 := time.Now().Sub(begin1).Seconds()
	log.Printf("time spent: %f s\n", elapsed1)

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
			log.Println(err)
			break loop
		default:
		}
		select {
		case r := <-fw2.Progress():
			r1, r2 := r.Folders, r.Files
			log.Printf("folders: %d files: %d\n", r1, r2)
			if r.Done {
				break loop
			}
		}
	}

	elapsed2 := time.Now().Sub(begin2).Seconds()
	log.Printf("time spent: %f s\n", elapsed2)

}

func getChange(d *watcher.DriveWatcher) {
	changes, e := d.GetDriveChanges()

	if e == nil {
		for _, i := range changes {
			if !i.Removed {
				log.Printf("change: %v %v %v %v %v\n", i.Time,
					i.File.Name, i.FileId, i.File.Parents,
					i.ChangeType)
			} else {
				log.Printf("change: %v %v %v\n", i.Time,
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
	defer lw.SendComd(watcher.C_CANCEL)
	for {
		for err := lw.SendComd(watcher.C_GET_CHANGE); err != nil; {
			log.Println(err)
			err = lw.SendComd(watcher.C_GET_CHANGE)
		}
		select {
		case err := <-lw.Error():
			if err != nil {
				log.Fatalln(err)
			}
		default:

		}

		ch := <-lw.ChangeChan()
		_ = ch
		for _, i := range ch {
			log.Println(*i)
		}

		time.Sleep(5 * time.Second)
	}

}

func profile() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
}

func testLocalDB() {
	go func() {
		store, err := localfs.Store()
	}
}

var userID string

func main() {
	profile()
	config, err := settings.ReadDriveConfig()
	log.Printf("setting error: %v\n", err)
	userID = config.Add("duckfat0000@gmail.com",
		"/home/kie/test")
	defer settings.SaveDriveConfig()

	// watchRemote()
	// remoteSync()
	// download()
	// mkdir()
	watchLocal()
	// localSync()

	//testLocalDB()

}
