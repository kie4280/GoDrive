package main

import (
	"fmt"
	"godrive/internal/drive"
	"time"
)

func main() {
	begin := time.Now()
	gclient := godrive.NewClient()
	sd := gclient.ListAll("dfg", "root")
	select {
	case r := <-sd:
		r1, r2 := r.Folders, r.Files
		fmt.Printf("folders: %d files: %d\n", r1, r2)

		elapsed := time.Now().Sub(begin).Seconds()
		fmt.Printf("time spent: %f s\n", elapsed)
	}

}
