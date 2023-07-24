package main

import "fmt"

// Test write across chunk boundary
func testWriteAcrossBoundary() {
	Create("test.txt")
	data := []byte{}
	// add "TEST" CHUNK_SIZE+1 times (each is 1 byte, so just across chunk boundary)
	for i := 0; i < CHUNK_SIZE+1; i++ {
		data = append(data, []byte("T")...)
	}
	Write("test.txt", 0, data)
}

// Test read across chunk boundary
func testReadAcrossBoundary() {
	content, err := Read("test.txt", CHUNK_SIZE, 1) // read the first letter in the second chunk
	if err != nil {
		fmt.Println("ERROR: Read Across Boundary Test Fails")
	}
	fmt.Println(string(content))
}

func main() {
}
