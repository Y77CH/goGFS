package main

import (
	"errors"
	"fmt"
	"net/rpc"
)

// the function that is used to simulate master
func getChunkHandleAndLocation(fileName string, chunkIndex int64) (chunkHandle, []string) {
	dummyList := *new([1]string)
	dummyList[0] = "127.0.0.1:12345"
	if fileName == "test" {
		if chunkIndex == 0 {
			return 0, dummyList[:]
		} else {
			return 1, dummyList[:]
		}
	}
	return 1, dummyList[:]
}

type chunkHandle int64
type ReadArgs struct {
	Handle chunkHandle
	Offset int64 // precision required by os seek function
	Length int
}
type ReadReturn []byte

// GFS client code that reads file starting at readStart byte for readLength bytes
func Read(fileName string, readStart int64, readLength int) (ReadReturn, error) {
	// TODO start server in separate processes

	// Use the assumed chunk size to calculate chunk index and chunk offset
	index := readStart / (64 * 1024 * 1024)
	var chunkOffset int64 = readStart % (64 * 1024 * 1024)
	fmt.Printf("INFO: Read started: File %s, starting at byte %d; Will read chunk %d, starting at byte %d\n", fileName, readStart, index, chunkOffset)
	// Get chunk handle and list of replicas from master
	handle, replicas := getChunkHandleAndLocation(fileName, index)

	// Start RPC client
	client, err := rpc.Dial("tcp", replicas[0]) // see design choices for explanation on replicas[0]
	if err != nil {
		fmt.Println("ERROR: " + err.Error())
	}
	// Prepare arguments and return values
	rpcArgs := ReadArgs{
		Handle: handle,
		Offset: chunkOffset,
		Length: readLength,
	}
	rpcReturn := []byte{}
	err = client.Call("ChunkServer.RPCRead", rpcArgs, &rpcReturn)
	if err != nil {
		fmt.Println("ERROR: In RPCRead: " + err.Error())
		return nil, errors.New("Read failed. Check logs") // see design choice
	} else {
		return rpcReturn, nil
	}
}
