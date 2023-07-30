package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"time"
)

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

// Test write of 1GB (1MB/Write)
func testWrite(filename *string) {
	// generate 1 MiB of data
	data := []byte{}
	for i := 0; i < 1024*1024/4; i++ {
		data = append(data, "TEST"...)
	}

	// create file for write
	Create(*filename)

	// write 1MiB of data for 1024 times (-> todal 1 GiB)
	start := time.Now()
	for i := 0; i < 1024; i++ {
		fmt.Printf("Writing %dth MB ======\n", i)
		err := Write(*filename, int64(i*1024*1024), data)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("Speed: At %v (second) this MB finished writing\n", time.Since(start).Seconds())
	}
	fmt.Println(time.Since(start))
}

// Read to verify write result (4MB/Read)
func verifyWrite(filename *string) {
	data := []byte{}
	for i := 0; i < 1024*1024; i++ { // 1024*1024*4/4
		data = append(data, "TEST"...)
	}
	for i := 0; i < 1024/4; i++ {
		fmt.Printf("Verifying %dth 4MB ======\n", i)
		ret, err := Read(*filename, int64(i*1024*1024*4), 1024*1024*4)
		if err != nil {
			fmt.Println(err)
		}
		if bytes.Equal(data, ret) {
			fmt.Println("Data correct")
		} else {
			// write to file for comparison when incorrect
			exp, err := os.Create("/var/gfs_test/expected")
			if err != nil {
				fmt.Println("Cannot create expected data file")
			}
			act, err := os.Create("/var/gfs_test/actual")
			if err != nil {
				fmt.Println("Cannot create actual data file")
			}
			act.WriteString(string(ret))
			exp.WriteString(string(data))
			fmt.Println("Data incorrect")
			break
		}
	}
}

func main() {
	filename := flag.String("f", "", "Specify the filename of test write")
	op := flag.String("op", "", "Specify the type of operation")
	flag.Parse()

	if *op == "w" {
		testWrite(filename)
	} else if *op == "v" {
		verifyWrite(filename)
	}
}
