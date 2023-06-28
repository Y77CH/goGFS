package main

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"strconv"
	"sync"
	"time"

	"os"
)

func (cs *ChunkServer) Read(args ReadArgs, reply *ReadReturn) error {
	fmt.Println("INFO: RPCRead invoked")

	// open file
	f, err := os.Open(cs.dataDir + fmt.Sprintf("%064d", args.Handle)) // pad int with 0s
	if err != nil {
		fmt.Println("ERROR: Open file failed")
		return err
	}
	// seek to correct offset
	f.Seek(args.Offset-1, 0) // need to minus one because seek treats the first byte as index 1
	content := make([]byte, args.Length)
	realLength, err := f.Read(content)
	if err != nil {
		return err
	}
	fmt.Println("INFO: File read. Expected length: " + strconv.Itoa(args.Length) + ". Real Length Read: " + strconv.Itoa(realLength) + ".")

	*reply = content
	return nil
}

// Called by client to push data to be written
func (cs *ChunkServer) DataPush(args DataPushArg, reply *DataPushReturn) error {
	fmt.Println("INFO: Data Push RPC invoked")

	// store data as buffer cache in memory
	id := bufferID{
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

// Client calls this function in primary to start append
func (cs *ChunkServer) PrimaryApplyAppend(args PrimaryApplyAppendArg, reply *PrimaryApplyAppendReturn) error {
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
		return errors.New("write to file failed")
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
			ChunkOffset:  fInfo.Size(),
			DataBufferID: args.AppendBufferIDs[i],
		}
		var applyAppendReturn ApplyMutationReturn
		err = client.Call("ChunkServer.ApplyMutate", applyAppendArg, &applyAppendReturn)
		if err != nil {
			fmt.Println("ERROR: Apply Append RPC call failed " + err.Error())
			return err
		}
	}

	return nil
}

// Client calls this function to start applying write
func (cs *ChunkServer) PrimaryApplyWrite(args PrimaryApplyWriteArg, reply *PrimaryApplyWriteReturn) error {
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
	_, err = f.WriteAt(toWrite, args.ChunkOffset)
	if err != nil {
		fmt.Println("INFO: Write to file failed with error: " + err.Error())
		return errors.New("write to file failed")
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
			ChunkOffset:  args.ChunkOffset,
			DataBufferID: args.WriteBufferIDs[i],
		}
		var applyWriteReturn ApplyMutationReturn
		err = client.Call("ChunkServer.ApplyMutate", applyWriteArg, &applyWriteReturn)
		if err != nil {
			fmt.Println("ERROR: Apply Append RPC call failed")
			return err
		}
	}

	return nil
}

// Primary calls this RPC to let the given chunkserver start writing / appending
func (cs *ChunkServer) ApplyMutate(args ApplyMutationArg, reply *ApplyMutationReturn) error {
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
	_, err = f.WriteAt(toWrite, int64(args.ChunkOffset))
	if err != nil {
		fmt.Println("INFO: Write to file failed with error: " + err.Error())
		return errors.New("write to file failed")
	}
	fmt.Println("INFO: Write to File Success")

	return nil
}

// INTERNAL FUNCTIONS ==========================================

// Called on bufferID to determine equality
func (id *bufferID) equal(toCompare bufferID) bool {
	if id.Timestamp.Compare(toCompare.Timestamp) == 0 && id.Handle == toCompare.Handle {
		return true
	} else {
		return false
	}
}

// Called by chunkserver to read its buffer
func (cs *ChunkServer) getBuffer(id bufferID) ([]byte, error) {
	cs.cache.lock.Lock()
	defer cs.cache.lock.Unlock()
	var toWrite []byte
	for k, v := range cs.cache.buffer { // must iterate to get value as go does not support custom equal
		if k.equal(id) {
			fmt.Println("INFO: Cache retrieved")
			toWrite = v
		}
	}
	if len(toWrite) == 0 {
		fmt.Println("ERROR: Cannot find data in cache")
		return nil, errors.New("cannot find data in cache")
	}
	return toWrite, nil
}

func (cs *ChunkServer) register(master string) error {
	client, err := rpc.Dial("tcp", master)
	if err != nil {
		return err
	}

	// Scan for available chunks
	handles := map[handle]int{}
	dir, err := os.ReadDir(cs.dataDir)
	if err != nil {
		return err
	}
	for _, f := range dir {
		h, err := strconv.Atoi(f.Name())
		if err != nil {
			return err
		}
		handles[handle(h)] = 0 // version number placeholder
	}

	// Prepare arguments of registration
	regArg := RegisterArgs{
		ChunkserverAddr: cs.addr,
		Handles:         handles,
	}
	var regRet RegisterReturn
	err = client.Call("MasterServer.Register", regArg, &regRet)
	if err != nil {
		return err
	}
	return nil
}

func startChunkServer(addr string, dataDir string, master string) error {
	// instantiate chunkserver
	var cache bufferCache = bufferCache{
		lock:   sync.RWMutex{},
		buffer: make(map[bufferID][]byte),
	}

	cs := &ChunkServer{
		dataDir: dataDir,
		addr:    addr,
		cache:   &cache,
	}

	// register and get listener
	rpc.Register(cs)
	listener, err := net.Listen("tcp", cs.addr)
	if err != nil {
		fmt.Println("ERROR: Server listener start failed. " + err.Error())
		return err
	}

	// register to master
	err = cs.register(master)
	if err != nil {
		return err
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
