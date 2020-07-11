package main

import (
	"fmt"
	godrive "godrive/internal/drive"
)

func main() {

	r1, r2 := godrive.ListAll("dfg")
	fmt.Printf("folders: %d files: %d", r1, r2)
}
