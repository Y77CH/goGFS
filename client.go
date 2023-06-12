package main

import (
	"errors"
	"fmt"
	"net/rpc"
	"time"
)

type chunkHandle int64
type ReadArgs struct {
	Handle chunkHandle
	Offset int64 // precision required by os seek function
	Length int
}
type ReadReturn []byte

// GFS client code that reads file starting at readStart byte for readLength bytes
func Read(fileName string, readStart int64, readLength int) (ReadReturn, error) {
	// Use the assumed chunk size to calculate chunk index and chunk offset
	index := readStart / (64 * 1024 * 1024)
	var chunkOffset int64 = readStart % (64 * 1024 * 1024)
	fmt.Printf("INFO: Read started: File %s, starting at byte %d; Will read chunk %d, starting at byte %d\n", fileName, readStart, index, chunkOffset)
	// Get chunk handle and list of replicas from master
	handle, replicas := GetChunkHandleAndLocation(fileName, index)
	fmt.Printf("INFO: Read Location Replied from Master: handle %d\n", handle)

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

type DataPushArg struct {
	Handle chunkHandle
	Data   []byte
}

type DataPushReturn struct {
	DataBufferID BufferID // will be -1 when unsuccessful
}

type BufferID struct {
	Handle    chunkHandle
	Timestamp time.Time
}

type PrimaryApplyAppendArg struct {
	Replicas        []string
	AppendBufferIDs []BufferID
}

type PrimaryApplyAppendReturn int // offset

// GFS client code that appends to the end of file.
func RecordAppend(fileName string, data []byte) (int, error) {
	// request chunk handle and replicas location
	handle, replicas := GetPrimaryAndReplicas(fileName)
	fmt.Printf("INFO: data will be appended at the end of %s, (chunk %d) on %d replicas \n", fileName, handle, len(replicas))
	// push data and prepare append
	var err error
	var applyAppendArg PrimaryApplyAppendArg
	applyAppendArg.AppendBufferIDs, err = push(replicas, handle, data)
	if err != nil {
		fmt.Println("ERROR: Data Push failed")
	}
	// apply append
	applyAppendArg.Replicas = replicas
	var applyAppendReturn PrimaryApplyAppendReturn
	primary, err := rpc.Dial("tcp", replicas[0])
	if err != nil {
		fmt.Println("ERROR: Dial primary to apply mutation failed")
	}
	err = primary.Call("ChunkServer.RPCPrimaryApplyAppend", applyAppendArg, &applyAppendReturn)
	if err != nil {
		fmt.Println("ERROR: Apply Append fails: " + err.Error())
		return -1, err
	}
	fmt.Printf("INFO: Appended at %d", int(applyAppendReturn)+int(handle)*64*1024*1024)
	return int(applyAppendReturn) + int(handle)*64*1024*1024, nil
}

type PrimaryApplyWriteArg struct {
	Replicas       []string
	WriteBufferIDs []BufferID
	Offset         int64
}

type PrimaryApplyWriteReturn int // placeholder

// GFS client code that writes to a certain place of file
func Write(fileName string, offset int, data []byte) error {
	// request chunk handle and replicas location
	// TODO get chunk location as read
	handle, replicas := GetPrimaryAndReplicas(fileName)
	// push data and prepare write
	var err error
	var applyWriteArg PrimaryApplyWriteArg
	applyWriteArg.WriteBufferIDs, err = push(replicas, handle, data)
	// for i := range applyWriteArg.AppendBufferIDs {
	// 	fmt.Printf("TEMP: Apply Write BufferIDs: ID: %d", i)
	// }
	if err != nil {
		fmt.Println("ERROR: Data Push failed")
	}
	// write data
	applyWriteArg.Replicas = replicas
	applyWriteArg.Offset = int64(offset)
	var applyWriteReturn PrimaryApplyWriteReturn
	primary, err := rpc.Dial("tcp", replicas[0])
	if err != nil {
		fmt.Println("ERROR: Dial primary to apply mutation failed")
	}
	err = primary.Call("ChunkServer.RPCPrimaryApplyWrite", applyWriteArg, &applyWriteReturn)
	if err != nil {
		fmt.Println("ERROR: Apply Write fails: " + err.Error())
		return err
	}
	fmt.Printf("INFO: Written at %d", int(applyWriteReturn)+int(handle)*64*1024*1024)
	return nil
}

// Internal APIs

// GFS client code that pushes data to each replica.
// Returns an array of buffer id and error (nil when success)
func push(replicas []string, handle chunkHandle, data []byte) ([]BufferID, error) {
	// prepare arguments and return
	appendArgs := DataPushArg{
		Handle: handle,
		Data:   data,
	}
	var pushReturn DataPushReturn
	var bufferIDs []BufferID
	// iterate all replicas to push data. return when any error happened
	var clients []*rpc.Client
	for _, replica := range replicas {
		client, err := rpc.Dial("tcp", replica)
		if err != nil {
			fmt.Println("ERROR: Dial Failed")
			return nil, err
		}
		clients = append(clients, client)
		err = client.Call("ChunkServer.RPCDataPush", appendArgs, &pushReturn)
		if err != nil {
			fmt.Println("ERROR: Data Push RPC Call failed: " + err.Error())
			return nil, err
		}
		fmt.Printf("INFO: Buffer ID received from %s\n", replica)
		bufferIDs = append(bufferIDs, pushReturn.DataBufferID)
	}
	return bufferIDs, nil
}
