package main

import (
	"errors"
	"fmt"
	"net/rpc"
)

// GFS client code that reads file starting at readStart byte for readLength bytes
func read(fileName string, readStart int64, readLength int) (ReadReturn, error) {
	// TODO start server in separate processes
	newServer("/tmp/")

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
