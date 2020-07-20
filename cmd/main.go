package main

import (
	"fmt"
	"godrive/internal/drive"
	"godrive/internal/watchers"
	"time"
)

func main() {
	begin1 := time.Now()
	gclient := godrive.NewClient("/home/kie/test")
	sd := gclient.ListAll("root")

	begin2 := time.Now()
	fw1 := fw.NewWatcher("/home/kie/test")
	fw2 := fw1.ListAll()

	select {
	case r := <-sd:
		r1, r2 := r.Folders, r.Files
		fmt.Printf("folders: %d files: %d\n", r1, r2)

		elapsed1 := time.Now().Sub(begin1).Seconds()
		fmt.Printf("time spent: %f s\n", elapsed1)
	}

	select {
	case r := <-fw2:
		r1, r2, err := r.Folders, r.Files, r.Error
		fmt.Printf("folders: %d files: %d error: %v\n", r1, r2, err)
	}
	elapsed2 := time.Now().Sub(begin2).Seconds()
	fmt.Printf("time spent: %f s\n", elapsed2)

}
