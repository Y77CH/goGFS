package main

import (
	"fmt"
	"time"
)

func testRead() {
	readContent, err := Read("test.txt", 64*1024*1024+1, 4)
	if err == nil {
		fmt.Println(readContent)
	} else {
		fmt.Println("Error: Reading Failed")
	}
}

func main() {
	// var wg sync.WaitGroup
	go testRead()
	go testRead()
	time.Sleep(time.Second)
}
