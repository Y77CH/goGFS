package main

import (
	"bytes"
	"flag"
	"fmt"
	"math/rand"
	"net/rpc"
	"os"
	"time"
)

// Test write of 1GB (1MB/Write)
func testSmallWrite(filename *string) {
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
		err := Write(*filename, int64(i*1024*1024), data)
		if err != nil {
			fmt.Println(err)
			break
		}
	}
	fmt.Println(time.Since(start))
	fmt.Println("(Small Write)")
}

// Test write of 1GB (at once)
func testLargeWrite(filename *string) {
	// generate 1 GiB of data
	data := []byte{}
	for i := 0; i < 1024*1024*1024/4; i++ {
		data = append(data, "TEST"...)
	}

	// create file for write
	Create(*filename)

	// write
	start := time.Now()
	err := Write(*filename, 0, data)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(time.Since(start))
	fmt.Println("(Large Write)")
}

// Read to verify write result (4MB/Read)
func verifyWrite(filename *string) {
	data := []byte{}
	for i := 0; i < 1024*1024; i++ { // 1024*1024*4/4
		data = append(data, "TEST"...)
	}
	for i := 0; i < 1024/4; i++ {
		ret, err := Read(*filename, int64(i*1024*1024*4), 1024*1024*4)
		if err != nil {
			fmt.Println(err)
		}
		if !bytes.Equal(data, ret) {
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
			fmt.Println("Data incorrect. Check /var/gfs_test/")
			break
		}
	}
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randStr(n int) string {
	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}

type DirectWriteArgs struct {
	Data     []byte
	Filename string
}

type DirectWriteReturn int

// Test the maximum performance: send file + write to disk (1 MB once)
func baselineTest(addr string) error {
	// Generate 1GB of data
	fmt.Println("Start to generate data")
	data := [1024][]byte{}
	for i := 0; i < 1024; i++ {
		data[i] = []byte(randStr(1024 * 1024))
	}

	// Prepare RPC
	fmt.Println("Start to dial")
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		fmt.Println("Error in dial")
		return err
	}
	// Directly write to chunkserver & record time
	fmt.Println("Start to transfer & write")
	start := time.Now()
	ret := DirectWriteReturn(0)
	for i := 0; i < 1024; i++ {
		err = client.Call("ChunkServer.DirectWrite", DirectWriteArgs{data[i], "baseline.txt"}, &ret)
		if err != nil {
			fmt.Println("Error in DirectWrite call")
			fmt.Println(err)
			return err
		}
	}
	// Show speed
	fmt.Print("Time Consumed: ")
	fmt.Println(time.Since(start))
	fmt.Print("Baseline speed: ")
	fmt.Print(1024 / time.Since(start).Seconds())
	fmt.Println(" MiB/s")
	return nil
}

func main() {
	filename := flag.String("f", "", "Specify the filename of test write")
	op := flag.String("op", "", "Specify the type of operation")
	node := flag.String("node", "", "Specify the node to run baseline test")
	master := flag.String("ms", "", "Specify the master address")
	flag.Parse()

	MASTER_ADDR = *master

	if *op == "w" {
		testSmallWrite(filename)
	} else if *op == "lw" {
		testLargeWrite(filename)
	} else if *op == "v" {
		verifyWrite(filename)
	} else if *op == "b" {
		baselineTest(*node)
	}
}
