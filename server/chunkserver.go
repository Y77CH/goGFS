package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/rpc"
	"strconv"
	"sync"
	"time"

	"os"
)

func (cs *ChunkServer) Read(args ReadArgs, reply *ReadReturn) error {
	fmt.Println("INFO: RPCRead invoked")
	if args.expire.Before(time.Now()) {
		cs.extensionBatch = append(cs.extensionBatch, args.Handle)
	}

	// open file
	f, err := os.Open(cs.dataDir + strconv.FormatUint(uint64(args.Handle), 10))
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
	if args.expire.Before(time.Now()) {
		cs.extensionBatch = append(cs.extensionBatch, args.AppendBufferIDs[0].Handle)
	}

	// TODO padding when necessary
	fmt.Println("INFO: Apply Append RPC invoked at primary")
	// retrieve data from buffer
	toWrite, err := cs.getBuffer(args.AppendBufferIDs[0])
	if err != nil {
		fmt.Println("ERROR: Cache retrieval failed.")
		return err
	}

	// apply append data to local and record offset of write
	f, err := os.OpenFile(cs.dataDir+strconv.FormatUint(uint64(args.AppendBufferIDs[0].Handle), 10), os.O_WRONLY, os.ModeAppend)
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
	cs.incVersion(args.AppendBufferIDs[0].Handle)
	if err != nil {
		fmt.Println("ERROR: inc version failed")
		return err
	}

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
	cs.extensionBatch = append(cs.extensionBatch, args.WriteBufferIDs[0].Handle)
	toWrite, err := cs.getBuffer(args.WriteBufferIDs[0])
	if err != nil {
		fmt.Println("ERROR: Cache retrieval failed.")
		return err
	}

	// apply writing data to local
	f, err := os.OpenFile(cs.dataDir+strconv.FormatUint(uint64(args.WriteBufferIDs[0].Handle), 10), os.O_WRONLY, os.ModeAppend)
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
	cs.incVersion(args.WriteBufferIDs[0].Handle)
	if err != nil {
		fmt.Println("ERROR: inc version failed")
		return err
	}

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
	f, err := os.OpenFile(cs.dataDir+strconv.FormatUint(uint64(args.DataBufferID.Handle), 10), os.O_WRONLY, os.ModeAppend)
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
	err = cs.incVersion(args.DataBufferID.Handle)
	if err != nil {
		fmt.Println("ERROR: inc version failed")
		return err
	}

	return nil
}

type chunkVersionMap map[handle]int // handle : version

// Called by master to check chunk and its version
func (cs *ChunkServer) HeartBeat(args HeartBeatArg, reply *HeartBeatReturn) error {
	versions, err := cs.loadVersion()
	if err != nil {
		return err
	}
	*reply = HeartBeatReturn{Version: versions[handle(args)]}
	for i := range cs.extensionBatch {
		if cs.extensionBatch[i] == handle(args) {
			reply.LeaseExtend = true
			cs.extensionBatch[i] = cs.extensionBatch[len(cs.extensionBatch)-1] // move the element to end
			cs.extensionBatch = cs.extensionBatch[:len(cs.extensionBatch)-1]   // remove the last element
		}
	}
	cs.lastHeatBeat = time.Now()
	return nil
}

func (cs *ChunkServer) NewChunk(args NewChunkArgs, ret *NewChunkReturn) error {
	// create new chunk file
	_, err := os.Create(cs.dataDir + strconv.FormatUint(uint64(args), 10))
	if err != nil {
		fmt.Println("ERROR: create new chunk file failed")
		return err
	}

	cs.incVersion(handle(args))

	return nil
}

func (cs *ChunkServer) DeleteChunk(args DelChunkArgs, ret *DelChunkReturn) error {
	err := os.Rename(cs.dataDir+strconv.FormatUint(uint64(args), 10), cs.dataDir+"."+strconv.FormatUint(uint64(args), 10))
	if err != nil {
		return err
	}
	return nil
}

// INTERNAL FUNCTIONS ==========================================

// Called by chunkserver to load its version info
func (cs *ChunkServer) loadVersion() (chunkVersionMap, error) {
	_, err := os.Stat(cs.dataDir + "versions.json")
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// create mapping file if not exist
			fmt.Println("INFO: Create new version mapping file")
			_, err = os.Create(cs.dataDir + "versions.json")
			if err != nil {
				return nil, err
			}
			return chunkVersionMap{}, nil
		} else {
			return nil, err
		}
	} else {
		// load mapping
		jsonVal, err := ioutil.ReadFile(cs.dataDir + "versions.json") // it's reasonable to read all at once due to the small size of metadata
		if err != nil {
			fmt.Println("ERROR: Read version file failed")
			return nil, err
		}
		var versions chunkVersionMap
		json.Unmarshal(jsonVal, &versions)
		return versions, nil
	}
}

// Called by chunkserver to update versions
func (cs *ChunkServer) incVersion(h handle) error {
	versions, err := cs.loadVersion()
	if err != nil {
		return err
	}
	if versions == nil {
		versions = chunkVersionMap{}
	}
	versions[h] += 1
	jsonVer, err := json.Marshal(versions)
	if err != nil {
		return err
	}
	f, err := os.Create(cs.dataDir + "versions.json")
	defer f.Close()
	if err != nil {
		return err
	}
	_, err = f.Write(jsonVer)
	if err != nil {
		return err
	}
	return nil
}

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
	// handles := map[handle]int{}
	// dir, err := os.ReadDir(cs.dataDir)
	// if err != nil {
	// 	return err
	// }
	// for _, f := range dir {
	// 	h, err := strconv.Atoi(f.Name())
	// 	if err != nil {
	// 		return err
	// 	}
	// 	handles[handle(h)] = 0 // version number placeholder
	// }
	vmap, err := cs.loadVersion()
	if err != nil {
		return err
	}

	// Prepare arguments of registration
	regArg := RegisterArgs{
		ChunkserverAddr: cs.addr,
		Handles:         vmap,
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
	cs.register(master) // no need to check error; will retry below

	wg := new(sync.WaitGroup)
	wg.Add(2)
	// concurrently handle requests
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				continue
			}
			go rpc.ServeConn(conn)
		}
	}(wg)

	// try reconnecting to master, when no heartbeat for a 2 * heatbeat interval
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		for {
			time.Sleep(HEARTBEAT_INTV * 2)
			if cs.lastHeatBeat.Add(2 * HEARTBEAT_INTV).Before(time.Now()) {
				// last heatbeat is 2 times interval before -> try connect
				fmt.Println("INFO: No heatbeat from master. Trying reconnection.")
				err := cs.register(master)
				for err != nil {
					fmt.Println("ERROR: " + err.Error())
					time.Sleep(HEARTBEAT_INTV)
					fmt.Println("INFO: Retrying")
					err = cs.register(master)
				}
				fmt.Println("INFO: Reconnection success")
			}
		}
	}(wg)
	wg.Wait()
	return nil
}
