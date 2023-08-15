package main

import (
	"bytes"
	"flag"
	"fmt"
	"math/rand"
	"net/rpc"
	"os"
	"strings"
	"time"
)

var HOSTNAME string

// Test write of 1GB (1MB/Write)
func testSmallWrite(filename string) {
	// generate 1 MiB of data
	data := [1024][]byte{}
	for i := 0; i < 1024; i++ {
		data[i] = []byte(randStr(1024 * 1024))
	}

	// create file for write
	Create(filename)

	// write 1MiB of data for 1024 times (-> todal 1 GiB)
	start := time.Now()
	for i := 0; i < 1024; i++ {
		err := Write(filename, int64(i*1024*1024), data[i])
		if err != nil {
			fmt.Println(err)
			break
		}
	}
	duration := time.Since(start)
	fmt.Printf("Small Write: time: %d seconds; speed: %.3f MB/s\n", int(duration.Seconds()), 1024/duration.Seconds())
}

// Test write of 1GB (at once)
func testLargeWrite(filename string) {
	// generate 1 GiB of data
	data := []byte(randStr(1024 * 1024 * 1024))

	// create file for write
	Create(filename)

	// write
	start := time.Now()
	err := Write(filename, 0, data)
	if err != nil {
		fmt.Println(err)
	}
	duration := time.Since(start)
	fmt.Printf("Large Write: time: %d seconds; speed: %.3f MB/s\n", int(duration.Seconds()), 1024/duration.Seconds())
}

// Read to verify write result (4MB/Read)
func verifyWrite(filename string) {
	data := []byte{}
	for i := 0; i < 1024*1024; i++ { // 1024*1024*4/4
		data = append(data, "TEST"...)
	}
	for i := 0; i < 1024/4; i++ {
		ret, err := Read(filename, int64(i*1024*1024*4), 1024*1024*4)
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
func smallWriteMax(addr1 string, addr2 string, addr3 string, filename string) {
	fmt.Printf("Testing max performance on %s, %s, %s\n", addr1, addr2, addr3)
	// Generate 1GB of data
	fmt.Println("Start to generate data")
	data := [1024][]byte{}
	for i := 0; i < 1024; i++ {
		data[i] = []byte(randStr(1024 * 1024))
	}

	// Prepare RPC
	fmt.Println("Start to dial")
	client1, err := rpc.Dial("tcp", addr1)
	if err != nil {
		panic(err)
	}
	client2, err := rpc.Dial("tcp", addr2)
	if err != nil {
		panic(err)
	}
	client3, err := rpc.Dial("tcp", addr3)
	if err != nil {
		panic(err)
	}
	clients := []*rpc.Client{client1, client2, client3}
	// Directly write to chunkserver & record time
	fmt.Println("Start to transfer & write")
	start := time.Now()
	ret := DirectWriteReturn(0)
	for i := 0; i < 3; i++ {
		for j := 0; j < 1024; j++ {
			err = clients[i].Call("ChunkServer.DirectWrite", DirectWriteArgs{data[j], filename}, &ret)
			if err != nil {
				panic(err)
			}
		}
	}
	// Show speed
	duration := time.Since(start)
	fmt.Printf("Small Write Baseline: time: %d seconds; speed: %.3f MB/s\n", int(duration.Seconds()), 1024/duration.Seconds())
}

// Test the maximum performance: send file + write to disk (1 GB once)
func largeWriteMax(addr1 string, addr2 string, addr3 string, filename string) {
	fmt.Printf("Testing max performance on %s, %s, %s\n", addr1, addr2, addr3)
	// Generate 1GB of data
	fmt.Println("Start to generate data")
	data := []byte(randStr(1024 * 1024 * 1024))

	// Prepare RPC
	fmt.Println("Start to dial")
	client1, err := rpc.Dial("tcp", addr1)
	if err != nil {
		panic(err)
	}
	client2, err := rpc.Dial("tcp", addr2)
	if err != nil {
		panic(err)
	}
	client3, err := rpc.Dial("tcp", addr3)
	if err != nil {
		panic(err)
	}
	clients := []*rpc.Client{client1, client2, client3}
	if err != nil {
		panic(err)
	}
	// Directly write to chunkserver & record time
	fmt.Println("Start to transfer & write")
	start := time.Now()
	ret := DirectWriteReturn(0)
	for i := 0; i < 3; i++ {
		err = clients[i].Call("ChunkServer.DirectWrite", DirectWriteArgs{data, filename}, &ret)
		if err != nil {
			panic(err)
		}
	}
	// Show speed
	duration := time.Since(start)
	fmt.Printf("Large Write Baseline: time: %d seconds; speed: %.3f MB/s\n", int(duration.Seconds()), 1024/duration.Seconds())
}

func smallFileMax(addr1 string, addr2 string, addr3 string, fileprefix string) {
	// Generate 1GB of data
	fmt.Println("Start to generate data")
	data := [1024][]byte{}
	for i := 0; i < 1024; i++ {
		data[i] = []byte(randStr(1024 * 1024))
	}

	// Prepare RPC
	fmt.Println("Start to dial")
	client1, err := rpc.Dial("tcp", addr1)
	if err != nil {
		panic(err)
	}
	client2, err := rpc.Dial("tcp", addr2)
	if err != nil {
		panic(err)
	}
	client3, err := rpc.Dial("tcp", addr3)
	if err != nil {
		panic(err)
	}
	clients := []*rpc.Client{client1, client2, client3}
	// Directly write to chunkserver & record time
	fmt.Println("Start to transfer & write")
	start := time.Now()
	ret := DirectWriteReturn(0)
	for i := 0; i < 3; i++ {
		for j := 0; j < 1024; j++ {
			err = clients[i].Call("ChunkServer.DirectWrite", DirectWriteArgs{data[j], fileprefix + fmt.Sprint(j) + ".txt"}, &ret)
			if err != nil {
				panic(err)
			}
		}
	}
	// Show speed
	duration := time.Since(start)
	fmt.Printf("Small Files Write Baseline: time: %d seconds; speed: %.3f MB/s\n", int(duration.Seconds()), 1024/duration.Seconds())
}

func smallFileTest(fileprefix string) {
	// generate 1 MiB of data
	data := [1024][]byte{}
	for i := 0; i < 1024; i++ {
		data[i] = []byte(randStr(1024 * 1024))
	}

	// write 1MiB of data for 1024 times (-> todal 1 GiB)
	start := time.Now()
	for i := 0; i < 1024; i++ {
		filename := fileprefix + fmt.Sprint(i) + ".txt"
		err := Create(filename)
		if err != nil {
			fmt.Println(i)
			fmt.Println(err)
			break
		}
		err = Write(filename, int64(i*1024*1024), data[i])
		if err != nil {
			fmt.Println(i)
			fmt.Println(err)
			break
		}
	}
	duration := time.Since(start)
	fmt.Printf("Small Files: time: %d seconds; speed: %.3f MB/s\n", int(duration.Seconds()), 1024/duration.Seconds())
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
		err = Write(HOSTNAME+"_small"+fmt.Sprint(i), 0, data)
		if err != nil {
			panic(err)
		}
		f.Close()
	}
	duration := time.Since(start)
	fmt.Printf("Small File Upload: time: %d seconds; speed: %.3f MB/s\n", int(duration.Seconds()), 1024/duration.Seconds())
}

type GetChunksListArgs int // placeholder
type GetChunksListReturn struct {
	Lengths []int    // list of corresponding chunk lengths
	Files   []string // list of chunk handles
}

func getChunksList(addr string) (*rpc.Client, GetChunksListReturn) {
	// Get list of chunks
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		panic(err)
	}
	ret := GetChunksListReturn{}
	err = client.Call("ChunkServer.GetChunksList", 0, &ret)
	if err != nil {
		panic(err)
	}
	return client, ret
}

type DirectReadArgs struct {
	Start int64 // read start (precision required by seek())
	Len   int   // length
	File  string
}
type DirectReadReturn []byte // read result

// Test random read baseline performance (4MB * 256)
func randomReadBaseline(addr string) {
	client, ret := getChunksList(addr)
	start := time.Now()
	// Random reads
	for i := 0; i < 256; i++ {
		j := rand.Intn(len(ret.Files)) // pick a random chunk
		if ret.Lengths[j]/(4*1024*1024) == 0 {
			i -= 1
			continue
		}
		k := rand.Intn(ret.Lengths[j] / (4 * 1024 * 1024)) // pick one random 4mb-piece
		content := DirectReadReturn{}
		err := client.Call("ChunkServer.DirectRead", DirectReadArgs{int64(k), 4 * 1024 * 1024, ret.Files[j]}, &content)
		if err != nil {
			panic(err)
		}
	}
	duration := time.Since(start)
	fmt.Printf("Random Read Baseline: time: %d seconds; speed: %3.f MB/s\n", int(duration.Seconds()), 1024/duration.Seconds())
}

func sequentialReadBaseline(addr string) {
	start := time.Now()
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		panic(err)
	}
	ret := DirectReadReturn{}
	client.Call("ChunkServer.DirectRead", DirectReadArgs{0, 1024 * 1024 * 1024, ""}, &ret) // filename == "" -> read all
	duration := time.Since(start)
	fmt.Printf("Sequential Read Baseline: time: %d seconds; speed: %.3f MB/s\n", int(duration.Seconds()), 1024/duration.Seconds())
}

// Setup fileset for read benchmarking (CloudLab constraint)
// note: fsize is in byte
func setupFileSet(fnum int, fsize int) {
	for i := 0; i < fnum; i++ {
		data := randStr(fsize)
		fmt.Println("Data generated")
		Write("readtest_"+fmt.Sprint(i)+".txt", 0, []byte(data))
		fmt.Println("Data written")
	}
}

// Test random read performance (4MB * 256)
// note: fsize is in byte
func testRandomRead(fnum int, fsize int) {
	start := time.Now()
	for i := 0; i < 256; i++ {
		j := rand.Intn(fnum)
		k := rand.Intn(fsize / 1024 / 1024 / 4)
		_, err := Read("readtest_"+fmt.Sprint(j)+".txt", int64(k), 4*1024*1024)
		if err != nil {
			panic(err)
		}
	}
	duration := time.Since(start)
	fmt.Printf("Random Read: time: %d seconds; speed: %.3f MB/s\n", int(duration.Seconds()), 1024/duration.Seconds())
}

// Test sequential read performance (1GB)
func testSequentialRead() {
	start := time.Now()
	_, err := Read("readtest_0.txt", 0, 1024*1024*1024)
	if err != nil {
		panic(err)
	}
	duration := time.Since(start)
	fmt.Printf("Sequential Read: time: %d seconds; speed: %.3f MB/s\n", int(duration.Seconds()), 1024/duration.Seconds())
}

func main() {
	// path := flag.String("f", "", "Specify the filename of test write")
	op := flag.String("op", "", "Specify the type of operation")
	nodes := flag.String("nodes", "", "Specify the node to run baseline test, separated by comma")
	master := flag.String("ms", "", "Specify the master address")
	flag.Parse()

	if *master == "" {
		panic("Empty master")
	}
	if len(strings.Split(*master, ":")) != 2 {
		panic("Invalid master")
	}

	MASTER_ADDR = *master
	HOSTNAME, err := os.Hostname()
	if err != nil {
		panic("Cannot get hostname")
	}

	nodes_list := strings.Split(*nodes, ",")

	if *op == "sw" {
		testSmallWrite(HOSTNAME + "_smallwrite.txt")
		// verifyWrite(HOSTNAME + "_smallwrite.txt")
	} else if *op == "lw" {
		testLargeWrite(HOSTNAME + "_largewrite.txt")
		// verifyWrite(HOSTNAME + "_largewrite.txt")
	} else if *op == "swmax" {
		smallWriteMax(nodes_list[0], nodes_list[1], nodes_list[2], HOSTNAME+"_smallwrite-max.txt")
	} else if *op == "lwmax" {
		largeWriteMax(nodes_list[0], nodes_list[1], nodes_list[2], HOSTNAME+"_largewrite-max.txt")
	} else if *op == "sf" {
		smallFileTest(HOSTNAME + "_smallfiles_")
	} else if *op == "sfmax" {
		smallFileMax(nodes_list[0], nodes_list[1], nodes_list[2], HOSTNAME+"_smallfile-max_")
	} else if *op == "setup" {
		setupFileSet(4, 1024*1024*1024)
	} else if *op == "rr" {
		testRandomRead(4, 1024*1024*1024)
	} else if *op == "sr" {
		testSequentialRead()
	} else if *op == "rrmax" {
		randomReadBaseline(nodes_list[0])
	} else if *op == "srmax" {
		sequentialReadBaseline(nodes_list[0])
	}
}
