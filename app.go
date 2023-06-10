package main

import "fmt"

func main() {
	readContent, err := read("test.txt", 64*1024*1024+1, 4)
	if err == nil {
		fmt.Println(readContent)
	}
}
