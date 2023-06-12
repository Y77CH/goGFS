package main

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"net/rpc"
	"strconv"
	"sync"
	"time"

	"os"
)

// {[handle, timestamp] : byte}
type bufferCache struct {
	lock   sync.RWMutex
	buffer map[BufferID][]byte
}

type ChunkServer struct {
	dataDir string
	addr    string
	cache   *bufferCache
}

type chunkHandle int64

type Chunk struct {
	handle   chunkHandle
	checksum string
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
	fmt.Println("INFO: File read. Expected length: " + strconv.Itoa(args.Length) + ". Real Length Read: " + strconv.Itoa(realLength) + ".")

	*reply = content
	return nil
}

type BufferID struct {
	Handle    chunkHandle
	Timestamp time.Time
}

func (id *BufferID) Equal(toCompare BufferID) bool {
	if id.Timestamp.Compare(toCompare.Timestamp) == 0 && id.Handle == toCompare.Handle {
		return true
	} else {
		return false
	}
}

type DataPushArg struct {
	Handle chunkHandle
	Data   []byte
}

type DataPushReturn struct {
	DataBufferID BufferID // will be -1 when unsuccessful
}

// Called by client to push data to be written
func (cs *ChunkServer) RPCDataPush(args DataPushArg, reply *DataPushReturn) error {
	fmt.Println("INFO: Data Push RPC invoked")

	// store data as buffer cache in memory
	id := BufferID{
		Handle:    args.Handle,
		Timestamp: time.Now(),
	}
	cs.cache.lock.Lock()
	defer cs.cache.lock.Unlock()
	// TODO Check for duplicacy first (same chunk, same time write)
	cs.cache.buffer[id] = args.Data
	// reply DataID for future access
	reply.DataBufferID = id
	fmt.Printf("INFO: Data saved in buffer cache with ID: handle: %d; timestamp: "+id.Timestamp.String()+"\n", id.Handle)
	return nil
}

type PrimaryApplyAppendArg struct {
	Replicas        []string
	AppendBufferIDs []BufferID
}

type PrimaryApplyAppendReturn int // offset

// Client calls this function in primary to start append
func (cs *ChunkServer) RPCPrimaryApplyAppend(args PrimaryApplyAppendArg, reply *PrimaryApplyAppendReturn) error {
	// TODO padding when necessary
	fmt.Println("INFO: Apply Append RPC invoked at primary")
	// retrieve data from buffer
	toWrite, err := cs.getBuffer(args.AppendBufferIDs[0])
	if err != nil {
		fmt.Println("ERROR: Cache retrieval failed.")
		return err
	}

	// apply append data to local and record offset of write
	f, err := os.OpenFile(cs.dataDir+fmt.Sprintf("%064d", args.AppendBufferIDs[0].Handle), os.O_WRONLY, os.ModeAppend)
	defer f.Close()
	if err != nil {
		fmt.Println("ERROR: open file failed")
	}
	fInfo, err := f.Stat()
	if err != nil {
		fmt.Println("ERROR: get file size failed")
	}
	_, err = f.WriteAt(toWrite, fInfo.Size())
	if err != nil {
		fmt.Println("INFO: Write to file failed with error: " + err.Error())
		return errors.New("Write to file failed")
	}
	fmt.Println("INFO: Write to File Success")
	*reply = PrimaryApplyAppendReturn(fInfo.Size())

	// forward apply append to all replicas
	for i := 1; i < len(args.AppendBufferIDs); i++ {
		client, err := rpc.Dial("tcp", args.Replicas[i])
		if err != nil {
			fmt.Println("ERROR: Dial Failed")
			return err
		}
		applyAppendArg := ApplyMutationArg{
			WriteOffset:  fInfo.Size(),
			DataBufferID: args.AppendBufferIDs[i],
		}
		var applyAppendReturn ApplyMutationReturn
		err = client.Call("ChunkServer.RPCApplyAppend", applyAppendArg, &applyAppendReturn)
		if err != nil {
			fmt.Println("ERROR: Apply Append RPC call failed")
			return err
		}
	}

	return nil
}

type PrimaryApplyWriteArg struct {
	Replicas       []string
	WriteBufferIDs []BufferID
	Offset         int64
}

type PrimaryApplyWriteReturn int // placeholder

// Client calls this function to start applying write
func (cs *ChunkServer) RPCPrimaryApplyWrite(args PrimaryApplyWriteArg, reply *PrimaryApplyWriteReturn) error {
	toWrite, err := cs.getBuffer(args.WriteBufferIDs[0])
	if err != nil {
		fmt.Println("ERROR: Cache retrieval failed.")
		return err
	}

	// apply writing data to local
	f, err := os.OpenFile(cs.dataDir+fmt.Sprintf("%064d", args.WriteBufferIDs[0].Handle), os.O_WRONLY, os.ModeAppend)
	defer f.Close()
	if err != nil {
		fmt.Println("ERROR: open file failed")
	}
	_, err = f.WriteAt(toWrite, args.Offset)
	if err != nil {
		fmt.Println("INFO: Write to file failed with error: " + err.Error())
		return errors.New("Write to file failed")
	}
	fmt.Println("INFO: Write to File Success")
	*reply = PrimaryApplyWriteReturn(0)

	// forward apply append to all replicas
	for i := 1; i < len(args.Replicas); i++ {
		client, err := rpc.Dial("tcp", args.Replicas[i])
		if err != nil {
			fmt.Println("ERROR: Dial Failed")
			return err
		}
		applyWriteArg := ApplyMutationArg{
			WriteOffset:  args.Offset,
			DataBufferID: args.WriteBufferIDs[i],
		}
		var applyWriteReturn ApplyMutationReturn
		err = client.Call("ChunkServer.RPCApplyAppend", applyWriteArg, &applyWriteReturn)
		if err != nil {
			fmt.Println("ERROR: Apply Append RPC call failed")
			return err
		}
	}

	return nil
}

type ApplyMutationArg struct {
	WriteOffset  int64 // required by WriteAt
	DataBufferID BufferID
}

type ApplyMutationReturn int // placeholder

// Primary calls this RPC to let the given chunkserver start writing / appending
func (cs *ChunkServer) RPCApplyAppend(args ApplyMutationArg, reply *ApplyMutationReturn) error {
	fmt.Println("INFO: Apply Append RPC invoked at chunkserver")
	// retrieve data from buffer
	toWrite, err := cs.getBuffer(args.DataBufferID)
	if err != nil {
		fmt.Println("ERROR: Cache retrieval failed.")
		return err
	}

	// apply append data to local and record offset of write
	f, err := os.OpenFile(cs.dataDir+fmt.Sprintf("%064d", args.DataBufferID.Handle), os.O_WRONLY, os.ModeAppend)
	defer f.Close()
	if err != nil {
		fmt.Println("ERROR: open file failed")
	}
	_, err = f.WriteAt(toWrite, int64(args.WriteOffset))
	if err != nil {
		fmt.Println("INFO: Write to file failed with error: " + err.Error())
		return errors.New("Write to file failed")
	}
	fmt.Println("INFO: Write to File Success")

	return nil
}

// Internal
func (cs *ChunkServer) getBuffer(id BufferID) ([]byte, error) {
	cs.cache.lock.Lock()
	defer cs.cache.lock.Unlock()
	var toWrite []byte
	for k, v := range cs.cache.buffer { // must iterate to get value as go does not support custom equal
		if k.Equal(id) {
			fmt.Println("INFO: Cache retrieved")
			toWrite = v
		}
	}
	if len(toWrite) == 0 {
		fmt.Println("ERROR: Cannot find data in cache")
		return nil, errors.New("Cannot find data in cache")
	}
	return toWrite, nil
}

// CMD
func main() {
	addr := flag.String("addr", "127.0.0.1:1234", "Specify the address where the chunkserver will be started at.")
	dataDir := flag.String("dir", "/tmp/", "Specify the directory path where the chunkserver will be reading and storing data.")
	flag.Parse()

	// instantiate chunkserver
	var cache bufferCache = bufferCache{
		lock:   sync.RWMutex{},
		buffer: make(map[BufferID][]byte),
	}

	cs := &ChunkServer{
		dataDir: *dataDir,
		addr:    *addr,
		cache:   &cache,
	}
	// register and get listener
	rpc.Register(cs)
	listener, err := net.Listen("tcp", cs.addr)
	if err != nil {
		fmt.Println("ERROR: Server listener start failed. " + err.Error())
	}
	// concurrently handle requests
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go rpc.ServeConn(conn)
	}
}
