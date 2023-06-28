package main

import (
	"errors"
	"fmt"
	"net/rpc"
	"time"
)

// DEFINITIONS =================================================

const MASTER_ADDR = "127.0.0.1:6666"

type handle int64

type bufferID struct {
	Handle    handle
	Timestamp time.Time
}

// RPC Arguments and Returns

type RegisterArgs struct {
	ChunkserverAddr string
	Handles         map[handle]int // handle : version
}

type RegisterReturn int //placeholder

type GetChunkArgs struct {
	FileName   string
	ChunkIndex int64
}

type GetChunkReturn struct {
	Handle       handle
	Chunkservers []string
}

type ReadArgs struct {
	Handle handle
	Offset int64 // precision required by os seek function
	Length int
}
type ReadReturn []byte

type DataPushArg struct {
	Handle handle
	Data   []byte
}

type DataPushReturn struct {
	DataBufferID bufferID // will be -1 when unsuccessful
}

type PrimaryApplyAppendArg struct {
	Replicas        []string
	AppendBufferIDs []bufferID
}

type PrimaryApplyAppendReturn int // offset

type PrimaryApplyWriteArg struct {
	Replicas       []string
	WriteBufferIDs []bufferID
	ChunkOffset    int64
	ChunkIndex     int
}

type PrimaryApplyWriteReturn int // placeholder

// PUBLIC API ==================================================

// GFS client code that reads file starting at readStart byte for readLength bytes
func Read(fileName string, readStart int64, readLength int) (ReadReturn, error) {
	// Use the assumed chunk size to calculate chunk index and chunk offset
	index := readStart / (64 * 1024 * 1024)
	var chunkOffset int64 = readStart % (64 * 1024 * 1024)
	fmt.Printf("INFO: Read started: File %s, starting at byte %d; Will read chunk %d, starting at byte %d\n", fileName, readStart, index, chunkOffset)
	// Get chunk handle and list of replicas from master
	client, err := rpc.Dial("tcp", MASTER_ADDR)
	if err != nil {
		return nil, err
	}
	getChunkArgs := GetChunkArgs{
		FileName:   fileName,
		ChunkIndex: index,
	}
	getChunkReturn := GetChunkReturn{}
	err = client.Call("MasterServer.GetChunkHandleAndLocations", getChunkArgs, &getChunkReturn)
	if err != nil {
		return nil, err
	}
	fmt.Println("INFO: Read Location Replied from Master")

	// Call chunkserver for read
	client, err = rpc.Dial("tcp", getChunkReturn.Chunkservers[0])
	if err != nil {
		fmt.Println("ERROR: " + err.Error())
	}
	readArgs := ReadArgs{
		Handle: getChunkReturn.Handle,
		Offset: chunkOffset,
		Length: readLength,
	}
	readReturn := []byte{}
	err = client.Call("ChunkServer.Read", readArgs, &readReturn)
	if err != nil {
		fmt.Println("ERROR: In Read: " + err.Error())
		return nil, errors.New("Read failed. Check logs") // see design choice
	} else {
		return readReturn, nil
	}
}

// GFS client code that appends to the end of file.
func RecordAppend(fileName string, data []byte) (int, error) {
	// request chunk handle and replicas location
	client, err := rpc.Dial("tcp", MASTER_ADDR)
	if err != nil {
		return -1, err
	}
	getChunkArgs := GetChunkArgs{
		FileName:   fileName,
		ChunkIndex: -1,
	}
	getChunkReturn := GetChunkReturn{}
	err = client.Call("MasterServer.GetChunkHandleAndLocations", getChunkArgs, &getChunkReturn)
	if err != nil {
		return -1, err
	}
	fmt.Printf("INFO: data will be appended at the end of %s, (chunk %d) on %d replicas \n", fileName, getChunkReturn.Handle, len(getChunkReturn.Chunkservers))
	// push data and prepare append
	var applyAppendArg PrimaryApplyAppendArg
	applyAppendArg.AppendBufferIDs, err = push(getChunkReturn.Chunkservers, getChunkReturn.Handle, data)
	if err != nil {
		fmt.Println("ERROR: Data Push failed")
	}
	// apply append
	applyAppendArg.Replicas = getChunkReturn.Chunkservers
	var applyAppendReturn PrimaryApplyAppendReturn
	primary, err := rpc.Dial("tcp", getChunkReturn.Chunkservers[0])
	if err != nil {
		fmt.Println("ERROR: Dial primary to apply mutation failed")
	}
	err = primary.Call("ChunkServer.PrimaryApplyAppend", applyAppendArg, &applyAppendReturn)
	if err != nil {
		fmt.Println("ERROR: Apply Append fails: " + err.Error())
		return -1, err
	}
	fmt.Printf("INFO: Appended at %d", int(applyAppendReturn)+int(getChunkReturn.Handle)*64*1024*1024)
	return int(applyAppendReturn) + int(getChunkReturn.Handle)*64*1024*1024, nil
}

// GFS client code that writes to a certain place of file. NOTE: file starts with byte 0
func Write(fileName string, offset int64, data []byte) error {
	index := offset / (64 * 1024 * 1024)
	var chunkOffset int64 = offset % (64 * 1024 * 1024)
	// request chunk handle and replicas location
	client, err := rpc.Dial("tcp", MASTER_ADDR)
	if err != nil {
		return err
	}
	getChunkArgs := GetChunkArgs{
		FileName:   fileName,
		ChunkIndex: index,
	}
	getChunkReturn := GetChunkReturn{}
	err = client.Call("MasterServer.GetChunkHandleAndLocations", getChunkArgs, &getChunkReturn)
	if err != nil {
		return err
	}
	// push data and prepare write
	var applyWriteArg PrimaryApplyWriteArg
	applyWriteArg.WriteBufferIDs, err = push(getChunkReturn.Chunkservers, getChunkReturn.Handle, data)
	// for i := range applyWriteArg.AppendBufferIDs {
	// 	fmt.Printf("TEMP: Apply Write BufferIDs: ID: %d", i)
	// }
	if err != nil {
		fmt.Println("ERROR: Data Push failed")
	}
	// write data
	applyWriteArg.Replicas = getChunkReturn.Chunkservers
	applyWriteArg.ChunkIndex = int(index)
	applyWriteArg.ChunkOffset = int64(chunkOffset)
	var applyWriteReturn PrimaryApplyWriteReturn
	primary, err := rpc.Dial("tcp", getChunkReturn.Chunkservers[0])
	if err != nil {
		fmt.Println("ERROR: Dial primary to apply mutation failed")
	}
	err = primary.Call("ChunkServer.PrimaryApplyWrite", applyWriteArg, &applyWriteReturn)
	if err != nil {
		fmt.Println("ERROR: Apply Write fails: " + err.Error())
		return err
	}
	return nil
}

// INTERNAL FUNCTIONS ==========================================

// GFS client code that pushes data to each replica.
// Returns an array of buffer id and error (nil when success)
func push(replicas []string, handle handle, data []byte) ([]bufferID, error) {
	// prepare arguments and return
	appendArgs := DataPushArg{
		Handle: handle,
		Data:   data,
	}
	var pushReturn DataPushReturn
	var bufferIDs []bufferID
	// iterate all replicas to push data. return when any error happened
	for _, replica := range replicas {
		client, err := rpc.Dial("tcp", replica)
		if err != nil {
			fmt.Println("ERROR: Dial Failed")
			return nil, err
		}
		err = client.Call("ChunkServer.DataPush", appendArgs, &pushReturn)
		if err != nil {
			fmt.Println("ERROR: Data Push RPC Call failed: " + err.Error())
			return nil, err
		}
		fmt.Printf("INFO: Buffer ID received from %s\n", replica)
		bufferIDs = append(bufferIDs, pushReturn.DataBufferID)
	}
	return bufferIDs, nil
}
