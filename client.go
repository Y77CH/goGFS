package main

import (
	"net/rpc"
	"time"
)

// DEFINITIONS =================================================

const CHUNK_SIZE_MB = 64
const CHUNK_SIZE = CHUNK_SIZE_MB * 1024 * 1024

var MASTER_ADDR string

type handle uint64

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
	ChunkIndex int
}

type GetChunkReturn struct {
	Handle       handle
	Chunkservers []string
	Expire       time.Time
}

type ReadArgs struct {
	Handle handle
	Offset int64 // precision required by os seek function
	Length int
	expire time.Time
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
	expire          time.Time
}

type PrimaryApplyAppendReturn int // offset

type PrimaryApplyWriteArg struct {
	Replicas       []string
	WriteBufferIDs []bufferID
	ChunkOffset    int64
	ChunkIndex     int
	expire         time.Time
}

type PrimaryApplyWriteReturn int // placeholder

type CreateArgs string // absolute path

type CreateReturn int // placeholder

type DelArgs string // absolute path

type DelReturn int // placeholder

// PUBLIC API ==================================================

func Read(fileName string, readStart int64, readLength int) (ReadReturn, error) {
	currChunkIndex := int(readStart / CHUNK_SIZE)
	currByteIndex := readStart % CHUNK_SIZE
	ret := []byte{}
	for readLength > 0 {
		remaining := int(CHUNK_SIZE - currByteIndex)
		if remaining <= readLength {
			// when more to read in next chunk -> process partially
			// fmt.Printf("currChunkIndex: %d; currByteIndex: %d; data Length (partial): %d\n", currChunkIndex, currByteIndex, remaining)
			chunkret, err := chunkRead(fileName, currChunkIndex, currByteIndex, remaining)
			if err != nil {
				return nil, err
			}
			ret = append(ret, []byte(chunkret)...)
			readLength -= int(remaining)
			currChunkIndex += 1
			currByteIndex = 0
		} else {
			// when all read can be done in current chunk -> process the entire data
			// fmt.Printf("currChunkIndex: %d; currByteIndex: %d; data Length: %d\n", currChunkIndex, currByteIndex, readLength)
			chunkret, err := chunkRead(fileName, currChunkIndex, currByteIndex, readLength)
			if err != nil {
				return nil, err
			}
			ret = append(ret, []byte(chunkret)...)
			readLength = 0
		}
	}
	return ret, nil
}

// GFS client code that appends to the end of file.
func RecordAppend(fileName string, data []byte) (int, error) {
	// Get chunk handle and list of replicas from master if necessary
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

	// push data and prepare append
	var applyAppendArg PrimaryApplyAppendArg
	applyAppendArg.AppendBufferIDs, err = push(getChunkReturn.Chunkservers, getChunkReturn.Handle, data)
	if err != nil {
		return -1, err
	}

	// apply append
	applyAppendArg.Replicas = getChunkReturn.Chunkservers
	applyAppendArg.expire = getChunkReturn.Expire
	var applyAppendReturn PrimaryApplyAppendReturn
	primary, err := rpc.Dial("tcp", getChunkReturn.Chunkservers[0])
	if err != nil {
		return -1, err
	}
	err = primary.Call("ChunkServer.PrimaryApplyAppend", applyAppendArg, &applyAppendReturn)
	if err != nil {
		return -1, err
	}
	return int(applyAppendReturn) + int(getChunkReturn.Handle)*64*1024*1024, nil
}

// Write data to fileName starting at writeStart
// NOTE: file starts with byte 0
func Write(fileName string, writeStart int64, data []byte) error {
	currChunkIndex := int(writeStart / CHUNK_SIZE)
	currByteIndex := writeStart % CHUNK_SIZE
	for len(data) > 0 {
		remaining := CHUNK_SIZE - currByteIndex
		if remaining <= int64(len(data)) {
			// when remaining space is not sufficient to hold entire data -> process partially
			chunkWrite(fileName, currChunkIndex, currByteIndex, data[:remaining])
			data = data[remaining:]
			currChunkIndex += 1
			currByteIndex = 0
		} else {
			// when remaining space can hold entire data -> process the entire data
			chunkWrite(fileName, currChunkIndex, currByteIndex, data)
			data = data[:0]
		}
	}
	return nil
}

func Create(filename string) error {
	// TODO Reuse master rpc client
	client, err := rpc.Dial("tcp", MASTER_ADDR)
	if err != nil {
		return err
	}
	arg := CreateArgs(filename)
	ret := CreateReturn(0)
	err = client.Call("MasterServer.Create", arg, &ret)
	if err != nil {
		return err
	}
	return nil
}

func Delete(filename string) error {
	// TODO Reuse master rpc client
	client, err := rpc.Dial("tcp", MASTER_ADDR)
	if err != nil {
		return err
	}
	arg := DelArgs(filename)
	ret := DelReturn(0)
	err = client.Call("MasterServer.Delete", arg, &ret)
	if err != nil {
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
			return nil, err
		}
		err = client.Call("ChunkServer.DataPush", appendArgs, &pushReturn)
		if err != nil {
			return nil, err
		}
		// fmt.Printf("INFO: Data of %d bytes pushed to server %s\n", len(data), replica)
		bufferIDs = append(bufferIDs, pushReturn.DataBufferID)
	}
	return bufferIDs, nil
}

// Write data to chunk index from startByte to endByte
func chunkWrite(fileName string, index int, startByte int64, data []byte) error {
	// Get chunk handle and list of replicas from master
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
	if err != nil {
		return err
	}
	// write data
	applyWriteArg.Replicas = getChunkReturn.Chunkservers
	applyWriteArg.ChunkIndex = int(index)
	applyWriteArg.ChunkOffset = int64(startByte)
	applyWriteArg.expire = getChunkReturn.Expire
	var applyWriteReturn PrimaryApplyWriteReturn
	primary, err := rpc.Dial("tcp", getChunkReturn.Chunkservers[0])
	if err != nil {
		return err
	}
	err = primary.Call("ChunkServer.PrimaryApplyWrite", applyWriteArg, &applyWriteReturn)
	if err != nil {
		return err
	}
	return nil
}

// GFS client code that reads file starting at readStart byte for readLength bytes
// Note: Read starts at byte 0
func chunkRead(fileName string, index int, startByte int64, readLen int) (ReadReturn, error) {
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

	// Call chunkserver for read
	client, err = rpc.Dial("tcp", getChunkReturn.Chunkservers[0])
	if err != nil {
		return nil, err
	}
	readArgs := ReadArgs{
		Handle: getChunkReturn.Handle,
		Offset: startByte,
		Length: readLen,
		expire: getChunkReturn.Expire,
	}
	readReturn := []byte{}
	err = client.Call("ChunkServer.Read", readArgs, &readReturn)
	if err != nil {
		return nil, err
	} else {
		return readReturn, nil
	}
}
