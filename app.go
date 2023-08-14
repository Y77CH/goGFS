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
func testSmallWrite() {
	// generate 1 MiB of data
	data := []byte{}
	for i := 0; i < 1024*1024/4; i++ {
		data = append(data, "TEST"...)
	}

	// create file for write
	filename := HOSTNAME + "_smallwrite.txt"
	Create(filename)

	// write 1MiB of data for 1024 times (-> todal 1 GiB)
	start := time.Now()
	for i := 0; i < 1024; i++ {
		err := Write(filename, int64(i*1024*1024), data)
		if err != nil {
			fmt.Println(err)
			break
		}
	}
	duration := time.Since(start)
	fmt.Printf("Small Write: time: %d seconds; speed: %d MB/s\n", int(duration.Seconds()), 1024/duration)
}

// Test write of 1GB (at once)
func testLargeWrite() {
	// generate 1 GiB of data
	data := []byte{}
	for i := 0; i < 1024*1024*1024/4; i++ {
		data = append(data, "TEST"...)
	}

	// create file for write
	filename := HOSTNAME + "_largewrite.txt"
	Create(filename)

	// write
	start := time.Now()
	err := Write(filename, 0, data)
	if err != nil {
		fmt.Println(err)
	}
	duration := time.Since(start)
	fmt.Printf("Large Write: time: %d seconds; speed: %d MB/s\n", int(duration.Seconds()), 1024/duration)
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
func smallWriteBaseline(addr1 string, addr2 string, addr3 string) {
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
			err = clients[i].Call("ChunkServer.DirectWrite", DirectWriteArgs{data[j], HOSTNAME + "_baseline.txt"}, &ret)
			if err != nil {
				panic(err)
			}
		}
	}
	// Show speed
	duration := time.Since(start)
	fmt.Printf("Small Write Baseline: time: %d seconds; speed: %d MB/s\n", int(duration.Seconds()), 1024/duration)
}

// Test the maximum performance: send file + write to disk (1 GB once)
func largeWriteBaseline(addr1 string, addr2 string, addr3 string) {
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
		err = clients[i].Call("ChunkServer.DirectWrite", DirectWriteArgs{data, HOSTNAME + "_baseline-large.txt"}, &ret)
		if err != nil {
			panic(err)
		}
	}
	// Show speed
	duration := time.Since(start)
	fmt.Printf("Large Write Baseline: time: %d seconds; speed: %d MB/s\n", int(duration.Seconds()), 1024/duration)
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
	fmt.Printf("Small File Upload: time: %d seconds; speed: %d MB/s\n", int(duration.Seconds()), 1024/duration)
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
		j := rand.Intn(len(ret.Files))                     // pick a random chunk
		k := rand.Intn(ret.Lengths[j] / (4 * 1024 * 1024)) // pick one random 4mb-piece
		if k == 0 {
			i -= 1
			continue
		}
		content := DirectReadReturn{}
		err := client.Call("ChunkServer.DirectRead", DirectReadArgs{int64(k), 4 * 1024 * 1024, ret.Files[j]}, &content)
		if err != nil {
			panic(err)
		}
	}
	duration := time.Since(start)
	fmt.Printf("Random Read Baseline: time: %d seconds; speed: %d MB/s\n", int(duration.Seconds()), 1024/duration)
}

// Test sequential read baseline performance (1GB)
func sequentialReadBaseline(addr string) {
	client, ret := getChunksList(addr)
	start := time.Now()

	alreadyRead := 0
	for i := 0; i < len(ret.Files); i++ {
		if 1024*1024*1024-alreadyRead <= ret.Lengths[i] {
			// finish after this reading
			client.Call("ChunkServer.DirectRead", DirectReadArgs{0, 1024*1024*1024 - alreadyRead, ret.Files[i]}, nil)
			break
		} else {
			// read completely and continue
			client.Call("ChunkServer.DirectRead", DirectReadArgs{0, ret.Lengths[i], ret.Files[i]}, nil)
			alreadyRead += ret.Lengths[i]
		}
	}
	if alreadyRead < 1024*1024*1024 {
		// after reading all chunks, still not satisfying 1GB
		panic("Total chunk size is less than 1GB")
	}
	duration := time.Since(start)
	fmt.Printf("Sequential Read Baseline: time: %d seconds; speed: %d MB/s\n", int(duration.Seconds()), 1024/duration)
}

// Setup 160G fileset for read benchmarking (CloudLab constraint)
func setupFileSet() {
	for i := 0; i < 160; i++ {
		data := randStr(1024 * 1024 * 1024)
		fmt.Println("Data generated")
		Write("test"+fmt.Sprint(i)+".txt", 0, []byte(data))
		fmt.Println("Data written")
	}
}

// Test random read performance (4MB * 256)
func testRandomRead() {
	start := time.Now()
	for i := 0; i < 256; i++ {
		j := rand.Intn(160)
		k := rand.Intn(1024 / 4)
		_, err := Read("test"+fmt.Sprint(j)+".txt", int64(k), 4*1024*1024)
		if err != nil {
			panic(err)
		}
	}
	duration := time.Since(start)
	fmt.Printf("Random Read: time: %d seconds; speed: %d MB/s\n", int(duration.Seconds()), 1024/duration)
}

// Test sequential read performance (1GB)
func testSequentialRead() {
	start := time.Now()
	_, err := Read("test1.txt", 0, 1024*1024*1024)
	if err != nil {
		panic(err)
	}
	duration := time.Since(start)
	fmt.Printf("Sequential Read: time: %d seconds; speed: %d MB/s\n", int(duration.Seconds()), 1024/duration)
}

func main() {
	path := flag.String("f", "", "Specify the filename of test write")
	op := flag.String("op", "", "Specify the type of operation")
	nodes := flag.String("node", "", "Specify the node to run baseline test, separated by ';'")
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
		testSmallWrite()
		verifyWrite(HOSTNAME + "_smallwrite.txt")
	} else if *op == "lw" {
		testLargeWrite()
		verifyWrite(HOSTNAME + "_largewrite.txt")
	} else if *op == "swb" {
		smallWriteBaseline(nodes_list[0], nodes_list[1], nodes_list[2])
	} else if *op == "lwb" {
		largeWriteBaseline(nodes_list[0], nodes_list[1], nodes_list[2])
	} else if *op == "su" {
		smallFileUploadTest(*path)
	} else if *op == "rr" {
		testRandomRead()
	} else if *op == "sr" {
		testSequentialRead()
	} else if *op == "rrb" {
		randomReadBaseline(nodes_list[0])
	} else if *op == "srb" {
		sequentialReadBaseline(nodes_list[0])
	} else if *op == "setup" {
		setupFileSet()
	}
}
