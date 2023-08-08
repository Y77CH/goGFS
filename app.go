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
func smallWriteBaselineTest(addr string) error {
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
	fmt.Print("Small Write Baseline speed: ")
	fmt.Print(1024 / time.Since(start).Seconds())
	fmt.Println(" MiB/s")
	return nil
}

// Test the maximum performance: send file + write to disk (1 GB once)
func largeWriteBaselineTest(addr string) error {
	// Generate 1GB of data
	fmt.Println("Start to generate data")
	data := []byte(randStr(1024 * 1024 * 1024))

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
	err = client.Call("ChunkServer.DirectWrite", DirectWriteArgs{data, "baseline-large.txt"}, &ret)
	if err != nil {
		fmt.Println("Error in DirectWrite call")
		fmt.Println(err)
		return err
	}
	// Show speed
	fmt.Print("Time Consumed: ")
	fmt.Println(time.Since(start))
	fmt.Print("Large Write Baseline speed: ")
	fmt.Print(1024 / time.Since(start).Seconds())
	fmt.Println(" MiB/s")
	return nil
}

// test the maximum performance: read file + send file + write to disk (1 mb once)
func smallFileUploadBaselineTest(addr string, filedir string) error {
	// generate small files
	fmt.Println("Start to generate data")
	for i := 0; i < 1024; i++ {
		f, err := os.Create(filedir + "small" + fmt.Sprint(i))
		if err != nil {
			panic(err)
		}
		_, err = f.Write([]byte(randStr(1024 * 1024)))
		if err != nil {
			panic(err)
		}
		f.Close()
	}
	// Prepare RPC
	fmt.Println("Start to dial")
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		fmt.Println("Error in dial")
		return err
	}
	// upload the files
	fmt.Println("Start to read & transfer & write")
	start := time.Now()
	for i := 0; i < 1024; i++ {
		data := make([]byte, 1024*1024)
		f, err := os.Open(filedir + "small" + fmt.Sprint(i))
		if err != nil {
			panic(err)
		}
		f.Read(data)
		ret := DirectWriteReturn(0)
		err = client.Call("ChunkServer.DirectWrite", DirectWriteArgs{data, randStr(5) + "-baseline-small-" + fmt.Sprint(i)}, &ret)
		if err != nil {
			panic(err)
		}
		f.Close()
	}
	// Show speed
	fmt.Print("Time Consumed: ")
	fmt.Println(time.Since(start))
	fmt.Print("Small Upload Baseline speed: ")
	fmt.Print(1024 / time.Since(start).Seconds())
	fmt.Println(" MiB/s")
	return nil
}

// Test the write performance (1G) when considering local IO
func largeFileUploadTest(filedir string) {
	f, err := os.Create(filedir + "large")
	if err != nil {
		panic(err)
	}

	_, err = f.Write([]byte(randStr(1024 * 1024 * 1024)))
	if err != nil {
		panic(err)
	}

	f.Close()

	start := time.Now()
	f, err = os.Open(filedir + "large")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	// "Upload" file at CHUNK_SIZE (to accomodate goseaweedfs)
	for i := 0; i < 16; i++ {
		data := make([]byte, 64*1024*1024)
		_, err = f.Read(data)
		if err != nil {
			panic(err)
		}
		// TODO add a better filename generation
		err := Write("large", int64(i*64*1024*1024), data)
		if err != nil {
			panic(err)
		}
	}
	fmt.Println(time.Since(start))
	fmt.Print("Large File Upload: ")
	fmt.Print(1024 / time.Since(start).Seconds())
	fmt.Println(" MiB/s")
}

// Test the write performance (1G; 1M/write) when considering local IO
func smallFileUploadTest(filedir string) {
	// generate small files
	for i := 0; i < 1024; i++ {
		f, err := os.Create(filedir + "small" + fmt.Sprint(i))
		if err != nil {
			panic(err)
		}
		_, err = f.Write([]byte(randStr(1024 * 1024)))
		if err != nil {
			panic(err)
		}
		f.Close()
	}

	// upload the files
	start := time.Now()
	for i := 0; i < 1024; i++ {
		data := make([]byte, 1024*1024)
		f, err := os.Open(filedir + "small" + fmt.Sprint(i))
		if err != nil {
			panic(err)
		}
		f.Read(data)
		err = Write(randStr(4)+"small"+fmt.Sprint(i), 0, data)
		if err != nil {
			panic(err)
		}
		f.Close()
	}
	fmt.Println(time.Since(start))
	fmt.Print("Small File Upload Speed: ")
	fmt.Print(1024 / time.Since(start).Seconds())
	fmt.Println(" MiB/s")
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
	} else if *op == "u" {
		smallFileUploadTest(*filename)
	} else if *op == "lu" {
		largeFileUploadTest(*filename)
	} else if *op == "sb" {
		smallWriteBaselineTest(*node)
	} else if *op == "lb" {
		largeWriteBaselineTest(*node)
	} else if *op == "ub" {
		smallFileUploadBaselineTest(*node, *filename)
	}
}
