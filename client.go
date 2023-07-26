package main

import (
	"fmt"
	"net/rpc"
	"time"
)

// DEFINITIONS =================================================

const MASTER_ADDR = "127.0.0.1:6666"
const CHUNK_SIZE_MB = 64
const CHUNK_SIZE = CHUNK_SIZE_MB * 1024 * 1024

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

var locations map[string]map[int]GetChunkReturn // filename : {index : {handle, servers, expire}}

// PUBLIC API ==================================================

func Read(fileName string, readStart int64, readLength int) (ReadReturn, error) {
	startIndex := int(readStart / CHUNK_SIZE)
	endIndex := int((readStart + int64(readLength)) / CHUNK_SIZE)
	i := startIndex
	ret := []byte{}
	for i < endIndex+1 {
		var start int64
		var end int64
		if i == startIndex {
			start = readStart % CHUNK_SIZE
			if startIndex == endIndex {
				end = (readStart + int64(readLength)) % CHUNK_SIZE
			} else {
				end = CHUNK_SIZE
			}
		} else if i == endIndex {
			start = 0
			end = (readStart + int64(readLength)) % CHUNK_SIZE
		} else {
			start = 0
			end = CHUNK_SIZE
		}
		chunkret, err := chunkRead(fileName, i, start, end)
		if err != nil {
			return nil, err
		}
		ret = append(ret, []byte(chunkret)...)
		i++
	}
	return ret, nil
}

// GFS client code that appends to the end of file.
func RecordAppend(fileName string, data []byte) (int, error) {
	// Get chunk handle and list of replicas from master if necessary
	getChunkReturn, exist := locations[fileName][-1]
	if !exist || getChunkReturn.Expire.Before(time.Now()) {
		client, err := rpc.Dial("tcp", MASTER_ADDR)
		if err != nil {
			return -1, err
		}
		getChunkArgs := GetChunkArgs{
			FileName:   fileName,
			ChunkIndex: -1,
		}
		getChunkReturn = GetChunkReturn{}
		err = client.Call("MasterServer.GetChunkHandleAndLocations", getChunkArgs, &getChunkReturn)
		if err != nil {
			return -1, err
		}
		if locations == nil {
			locations = map[string]map[int]GetChunkReturn{}
		}
		if locations[getChunkArgs.FileName] == nil {
			locations[getChunkArgs.FileName] = map[int]GetChunkReturn{}
		}
		locations[getChunkArgs.FileName][getChunkArgs.ChunkIndex] = getChunkReturn // add to known locations
	}

	// push data and prepare append
	var applyAppendArg PrimaryApplyAppendArg
	var err error
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
	fmt.Println("Record Append success")
	return int(applyAppendReturn) + int(getChunkReturn.Handle)*64*1024*1024, nil
}

// Write data to fileName starting at writeStart
// NOTE: file starts with byte 0
func Write(fileName string, writeStart int64, data []byte) error {
	startIndex := int(writeStart / CHUNK_SIZE)
	endIndex := (int(writeStart) + len(data)) / CHUNK_SIZE
	i := startIndex

	for i < endIndex+1 {
		var start int64
		var end int64
		if i == startIndex {
			start = writeStart % CHUNK_SIZE
			end = CHUNK_SIZE
		} else if i == endIndex {
			start = 0
			end = (writeStart + int64(len(data))) % CHUNK_SIZE
		} else {
			start = 0
			end = CHUNK_SIZE
		}
		length := end - start
		toWrite := data[:length]
		data = data[length:]
		err := chunkWrite(fileName, i, start, end, toWrite)
		if err != nil {
			return err
		}
		i++
	}
	fmt.Println("INFO: Write success")
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
	fmt.Println("INFO: Create success")
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
	fmt.Println("INFO: Delete success")
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
		bufferIDs = append(bufferIDs, pushReturn.DataBufferID)
	}
	return bufferIDs, nil
}

// Write data to chunk index from startByte to endByte
func chunkWrite(fileName string, index int, startByte int64, endByte int64, data []byte) error {
	// Get chunk handle and list of replicas from master if necessary
	getChunkReturn, exist := locations[fileName][index]
	if !exist || getChunkReturn.Expire.Before(time.Now()) {
		client, err := rpc.Dial("tcp", MASTER_ADDR)
		if err != nil {
			return err
		}
		getChunkArgs := GetChunkArgs{
			FileName:   fileName,
			ChunkIndex: index,
		}
		getChunkReturn = GetChunkReturn{}
		err = client.Call("MasterServer.GetChunkHandleAndLocations", getChunkArgs, &getChunkReturn)
		if err != nil {
			return err
		}
		if locations == nil {
			locations = map[string]map[int]GetChunkReturn{}
		}
		if locations[getChunkArgs.FileName] == nil {
			locations[getChunkArgs.FileName] = map[int]GetChunkReturn{}
		}
		locations[getChunkArgs.FileName][getChunkArgs.ChunkIndex] = getChunkReturn // add to known locations
	}

	// push data and prepare write
	var applyWriteArg PrimaryApplyWriteArg
	var err error
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
	fmt.Printf("INFO: Write success on chunk %d of file %s (handle %d)\n", index, fileName, locations[fileName][index].Handle)
	return nil
}

// GFS client code that reads file starting at readStart byte for readLength bytes
// Note: Read starts at byte 0
func chunkRead(fileName string, index int, startByte int64, endByte int64) (ReadReturn, error) {
	// Get chunk handle and list of replicas from master if necessary
	getChunkReturn, exist := locations[fileName][index]
	if !exist || getChunkReturn.Expire.Before(time.Now()) {
		// no entry or expired entry for given file and index, ask master
		client, err := rpc.Dial("tcp", MASTER_ADDR)
		if err != nil {
			return nil, err
		}
		getChunkArgs := GetChunkArgs{
			FileName:   fileName,
			ChunkIndex: index,
		}
		getChunkReturn = GetChunkReturn{}
		err = client.Call("MasterServer.GetChunkHandleAndLocations", getChunkArgs, &getChunkReturn)
		if err != nil {
			return nil, err
		}
		// initialize locations if necessary
		if locations == nil {
			locations = map[string]map[int]GetChunkReturn{}
		}
		if locations[getChunkArgs.FileName] == nil {
			locations[getChunkArgs.FileName] = map[int]GetChunkReturn{}
		}
		locations[getChunkArgs.FileName][getChunkArgs.ChunkIndex] = getChunkReturn // add to known locations
	}

	// Call chunkserver for read
	client, err := rpc.Dial("tcp", getChunkReturn.Chunkservers[0])
	if err != nil {
		return nil, err
	}
	readArgs := ReadArgs{
		Handle: getChunkReturn.Handle,
		Offset: startByte,
		Length: int(endByte - startByte),
		expire: getChunkReturn.Expire,
	}
	readReturn := []byte{}
	err = client.Call("ChunkServer.Read", readArgs, &readReturn)
	if err != nil {
		return nil, err
	} else {
		fmt.Printf("INFO: Read success on chunk %d of file %s (handle %d)\n", index, fileName, locations[fileName][index].Handle)
		return readReturn, nil
	}
}
