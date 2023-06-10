package main

import (
	"fmt"
	"net"
	"net/rpc"
	"strconv"

	// "crypto/md5"
	"os"
)

type ChunkServer struct {
	dataDir string
	addr    string
}

type chunkHandle int64

type Chunk struct {
	handle   chunkHandle
	checksum string
}

func newServer(dataDir string) {
	// instantiate chunkserver
	cs := &ChunkServer{
		dataDir: dataDir,
		addr:    "127.0.0.1:12345", // TODO dummy IP address
	}
	// register and get listener
	rpc.Register(cs)
	listener, err := net.Listen("tcp", cs.addr)
	if err != nil {
		fmt.Println("ERROR: Server listener start failed. " + err.Error())
	}
	// start listener concurrently
	go func() {
		rpc.Accept(listener)
	}()
}

// RPC
type ReadArgs struct {
	Handle chunkHandle
	Offset int64 // precision required by os seek function
	Length int
}
type ReadReturn []byte

func (cs *ChunkServer) RPCRead(args ReadArgs, reply *ReadReturn) error {
	fmt.Println("INFO: RPCRead invoked")

	// open file
	f, err := os.Open(cs.dataDir + fmt.Sprintf("%064d", args.Handle)) // pad int with 0s
	if err != nil {
		fmt.Println("ERROR: Open file failed")
	}
	// seek to correct offset
	f.Seek(args.Offset-1, 0) // need to minus one because seek treats the first byte as index 1
	content := make([]byte, args.Length)
	realLength, err := f.Read(content)
	// fmt.Printf("TEMP: content read: %s\n", content)
	fmt.Println("INFO: File read. Expected length: " + strconv.Itoa(args.Length) + ". Real Length Read: " + strconv.Itoa(realLength) + ".")

	*reply = content
	return nil
}

// Internal
