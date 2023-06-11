package main

import (
	"fmt"
	"sync"
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
	// go testRead()
	// go testRead()
	// time.Sleep(time.Second)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			testRead()
		}(i)
	}
	wg.Wait()
}
