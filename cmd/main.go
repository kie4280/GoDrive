package main

import (
	"fmt"
	godrive "godrive/internal/drive"
)

func main() {

	r := godrive.ListAll("dfg")
	fmt.Println("Files:")
	if len(r.Files) == 0 {
		fmt.Println("No files found.")
	} else {
		for _, i := range r.Files {
			fmt.Printf("%s (%s)\n", i.Name, i.Id)
		}
	}
}
