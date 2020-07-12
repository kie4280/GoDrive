package main

import (
	"fmt"
	"godrive/internal/drive"
	"time"
)

func main() {
	begin := time.Now()
	r1, r2 := godrive.ListAll("dfg")
	fmt.Printf("folders: %d files: %d\n", r1, r2)

	elapsed := time.Now().Sub(begin).Seconds()
	fmt.Printf("time spent: %f s\n", elapsed)
}
