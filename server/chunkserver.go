package main

import (
	"encoding/json"
	"errors"
	"net"
	"net/rpc"
	"strconv"
	"sync"
	"time"

	"os"

	"go.uber.org/zap"
)

func (cs *ChunkServer) Read(args ReadArgs, reply *ReadReturn) error {
	zap.L().Info("Read started")
	if args.expire.Before(time.Now()) {
		cs.extensionBatch = append(cs.extensionBatch, args.Handle)
	}

	// open file
	f, err := os.Open(cs.dataDir + strconv.FormatUint(uint64(args.Handle), 10))
	if err != nil {
		zap.L().Fatal("Cannot open chunk file for read")
		return err
	}
	// seek to correct offset
	f.Seek(args.Offset, 0)
	content := make([]byte, args.Length)
	_, err = f.Read(content)
	if err != nil {
		zap.L().Fatal("Cannot read file")
		return err
	}

	*reply = content
	zap.L().Info("Read finished")
	return nil
}

// Called by client to push data to be written
func (cs *ChunkServer) DataPush(args DataPushArg, reply *DataPushReturn) error {
	zap.L().Info("Data push started",
		zap.Uint64("handle", uint64(args.Handle)))

	// store data as buffer cache in memory
	id := bufferID{
		Handle:    args.Handle,
		Timestamp: time.Now(),
	}
	// TODO Check for duplicacy first (same chunk, same time write)
	cs.cache.lock.Lock()
	cs.cache.buffer[id] = args.Data
	cs.cache.lock.Unlock()
	// reply DataID for future access
	reply.DataBufferID = id
	zap.L().Info("DataPush finished")
	return nil
}

// Client calls this function in primary to start append
func (cs *ChunkServer) PrimaryApplyAppend(args PrimaryApplyAppendArg, reply *PrimaryApplyAppendReturn) error {
	zap.L().Info("PrimaryApplyAppend started")
	if args.expire.Before(time.Now()) {
		cs.extensionBatch = append(cs.extensionBatch, args.AppendBufferIDs[0].Handle)
	}

	// TODO padding when necessary
	// retrieve data from buffer
	toWrite, err := cs.getBuffer(args.AppendBufferIDs[0])
	if err != nil {
		zap.L().Fatal("Failed to retrieve cache")
		return err
	}

	// apply append data to local and record offset of write
	f, err := os.OpenFile(cs.dataDir+strconv.FormatUint(uint64(args.AppendBufferIDs[0].Handle), 10), os.O_WRONLY, os.ModeAppend)
	if err != nil {
		zap.L().Fatal("Open file failed")
		return err
	}
	defer f.Close()
	fInfo, err := f.Stat()
	if err != nil {
		zap.L().Fatal("Get file size failed")
		return err
	}
	_, err = f.WriteAt(toWrite, fInfo.Size())
	if err != nil {
		zap.L().Fatal("Write to file failed")
		return err
	}
	*reply = PrimaryApplyAppendReturn(fInfo.Size())
	cs.incVersion(args.AppendBufferIDs[0].Handle)
	if err != nil {
		zap.L().Fatal("Increment version failed")
		return err
	}

	// forward apply append to all replicas
	for i := 1; i < len(args.AppendBufferIDs); i++ {
		client, err := rpc.Dial("tcp", args.Replicas[i])
		if err != nil {
			zap.L().Fatal("Dial failed")
			return err
		}
		applyAppendArg := ApplyMutationArg{
			ChunkOffset:  fInfo.Size(),
			DataBufferID: args.AppendBufferIDs[i],
		}
		var applyAppendReturn ApplyMutationReturn
		err = client.Call("ChunkServer.ApplyMutate", applyAppendArg, &applyAppendReturn)
		if err != nil {
			zap.L().Fatal("RPC call failed")
			return err
		}
	}

	zap.L().Info("Request all chunkservers to apply append success", zap.Uint64("handle", uint64(args.AppendBufferIDs[0].Handle)))
	zap.L().Info("PrimaryApplyAppend finished")
	return nil
}

// Client calls this function to start applying write
func (cs *ChunkServer) PrimaryApplyWrite(args PrimaryApplyWriteArg, reply *PrimaryApplyWriteReturn) error {
	zap.L().Info("PrimaryApplyWrite started")

	cs.extensionBatch = append(cs.extensionBatch, args.WriteBufferIDs[0].Handle)
	toWrite, err := cs.getBuffer(args.WriteBufferIDs[0])
	if err != nil {
		zap.L().Fatal("Cache retrieval failed")
		return err
	}

	// write data and increment version
	err = write(cs.dataDir+strconv.FormatUint(uint64(args.WriteBufferIDs[0].Handle), 10), toWrite, args.ChunkOffset)
	if err != nil {
		return err
	}
	zap.L().Info("Write to file success", zap.Uint64("handle", uint64(args.WriteBufferIDs[0].Handle)))
	cs.incVersion(args.WriteBufferIDs[0].Handle)
	if err != nil {
		return err
	}

	// forward apply append to all replicas
	for i := 1; i < len(args.Replicas); i++ {
		client, err := rpc.Dial("tcp", args.Replicas[i])
		if err != nil {
			zap.L().Fatal("Dial failed")
			return err
		}
		applyWriteArg := ApplyMutationArg{
			ChunkOffset:  args.ChunkOffset,
			DataBufferID: args.WriteBufferIDs[i],
		}
		var applyWriteReturn ApplyMutationReturn
		err = client.Call("ChunkServer.ApplyMutate", applyWriteArg, &applyWriteReturn)
		if err != nil {
			zap.L().Fatal("RPC call failed")
			return err
		}
	}

	zap.L().Info("Request all chunkservers to apply write success", zap.Uint64("handle", uint64(args.WriteBufferIDs[0].Handle)))
	zap.L().Info("PrimaryApplyWrite finished")
	return nil
}

// Primary calls this RPC to let the given chunkserver start writing / appending
func (cs *ChunkServer) ApplyMutate(args ApplyMutationArg, reply *ApplyMutationReturn) error {
	zap.L().Info("ApplyMutate started")
	// retrieve data from buffer
	toWrite, err := cs.getBuffer(args.DataBufferID)
	if err != nil {
		zap.L().Fatal("Cache retrieval failed")
		return err
	}

	// apply append data to local and record offset of write
	err = write(cs.dataDir+strconv.FormatUint(uint64(args.DataBufferID.Handle), 10), toWrite, args.ChunkOffset)
	if err != nil {
		return err
	}

	zap.L().Info("Write to file success", zap.Uint64("handle", uint64(args.DataBufferID.Handle)))
	err = cs.incVersion(args.DataBufferID.Handle)
	if err != nil {
		zap.L().Fatal("Increment version failed")
		return err
	}

	zap.L().Info("ApplyMutate finished")
	return nil
}

type chunkVersionMap map[handle]int // handle : version

// Called by master to check chunk and its version
func (cs *ChunkServer) HeartBeat(args HeartBeatArg, reply *HeartBeatReturn) error {
	versions, err := cs.loadVersion()
	if err != nil {
		zap.L().Fatal("Load version failed")
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
	zap.L().Info("NewChunk started")
	// create new chunk file
	_, err := os.Create(cs.dataDir + strconv.FormatUint(uint64(args), 10))
	if err != nil {
		zap.L().Fatal("Create new chunk file failed")
		return err
	}

	cs.incVersion(handle(args))

	zap.L().Info("NewChunk completed")
	return nil
}

func (cs *ChunkServer) DeleteChunk(args DelChunkArgs, ret *DelChunkReturn) error {
	zap.L().Info("DeleteChunk started")
	err := os.Rename(cs.dataDir+strconv.FormatUint(uint64(args), 10), cs.dataDir+"."+strconv.FormatUint(uint64(args), 10))
	if err != nil {
		zap.L().Fatal("Delete chunk failed")
		return err
	}
	zap.L().Info("DeleteChunk finished")
	return nil
}

type DirectWriteArgs struct {
	Data     []byte
	Filename string
}

type DirectWriteReturn int

// Called directly by client to benchmark maximum data transfer + write speed
func (cs *ChunkServer) DirectWrite(args DirectWriteArgs, ret *DirectWriteReturn) error {
	zap.L().Info("DirectWrite benchmarking started")
	f, err := os.OpenFile(cs.dataDir+args.Filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		zap.L().Fatal("Query file info failed", zap.String("name", cs.dataDir+args.Filename))
		return err
	}
	defer f.Close()
	_, err = f.Write(args.Data)
	if err != nil {
		zap.L().Fatal("Write file failed", zap.String("name", cs.dataDir+args.Filename))
		return err
	}
	return nil
}

type GetChunksListArgs int // placeholder
type GetChunksListReturn struct {
	Lengths []int    // list of corresponding chunk lengths
	Files   []string // list of chunk handles
}

// Called directly by client to collect chunk info (for benchmarking)
func (cs *ChunkServer) GetChunksList(args GetChunksListArgs, ret *GetChunksListReturn) error {
	// because data directly setup by client, cannot use loadVersion
	files, err := os.ReadDir(cs.dataDir)
	if err != nil {
		zap.L().Fatal("Directory cannot be opened to get list of available files")
	}
	for _, f := range files {
		ret.Files = append(ret.Files, f.Name())
		finfo, err := f.Info()
		if err != nil {
			zap.L().Fatal("Query file info failed", zap.String("name", f.Name()))
			return err
		}
		ret.Lengths = append(ret.Lengths, int(finfo.Size()))
	}
	return nil
}

type DirectReadArgs struct {
	Start int64 // read start (precision required by seek())
	Len   int   // length
	File  string
}
type DirectReadReturn []byte // read result

// Called directly by client to read a certain byte range of a given handle (for benchmarking)
func (cs *ChunkServer) DirectRead(args DirectReadArgs, ret *DirectReadReturn) error {
	f, err := os.Open(cs.dataDir + args.File)
	if err != nil {
		zap.L().Fatal("Cannot open file for read", zap.String("name", args.File))
		return err
	}

	// seek to correct offset
	f.Seek(args.Start, 0)
	content := make([]byte, args.Len)
	_, err = f.Read(content)
	if err != nil {
		zap.L().Fatal("Cannot read file")
		return err
	}

	*ret = content
	return nil
}

// INTERNAL FUNCTIONS ==========================================

// Called by chunkserver to load its version info
func (cs *ChunkServer) loadVersion() (chunkVersionMap, error) {
	_, err := os.Stat(cs.dataDir + "versions.json")
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// create mapping file if not exist
			zap.L().Info("Version mapping file not exist. Creating.")
			_, err = os.Create(cs.dataDir + "versions.json")
			if err != nil {
				zap.L().Fatal("Create new version mapping file failed")
				return nil, err
			}
			return chunkVersionMap{}, nil
		} else {
			return nil, err
		}
	} else {
		// load mapping
		jsonVal, err := os.ReadFile(cs.dataDir + "versions.json") // it's reasonable to read all at once due to the small size of metadata
		if err != nil {
			zap.L().Fatal("Read version file failed")
			return nil, err
		}
		var versions chunkVersionMap
		json.Unmarshal(jsonVal, &versions)
		return versions, nil
	}
}

// Called by chunkserver to update versions
func (cs *ChunkServer) incVersion(h handle) error {
	zap.L().Debug("incVersion started")
	versions, err := cs.loadVersion()
	if err != nil {
		zap.L().Fatal("Load version failed")
		return err
	}
	if versions == nil {
		versions = chunkVersionMap{}
	}
	versions[h] += 1
	jsonVer, err := json.Marshal(versions)
	if err != nil {
		zap.L().Fatal("Parse version file failed")
		return err
	}
	f, err := os.Create(cs.dataDir + "versions.json")
	if err != nil {
		zap.L().Fatal("Create versions file failed")
		return err
	}
	defer f.Close()
	_, err = f.Write(jsonVer)
	if err != nil {
		zap.L().Fatal("Write to versions file failed")
		return err
	}

	zap.L().Debug("incVersion finished")
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

// Called by chunkserver to read its buffer and delete it
func (cs *ChunkServer) getBuffer(id bufferID) ([]byte, error) {
	zap.L().Debug("getBuffer started")
	cs.cache.lock.Lock()
	defer cs.cache.lock.Unlock()
	var toWrite []byte
	for k, v := range cs.cache.buffer { // must iterate to get value as go does not support custom equal
		if k.equal(id) {
			toWrite = v
			delete(cs.cache.buffer, k)
		}
	}
	if toWrite == nil {
		zap.L().Fatal("Data not found in cache", zap.Uint64("handle", uint64(id.Handle)), zap.Time("timestamp", id.Timestamp))
		return nil, errors.New("cannot find data in cache")
	}
	zap.L().Debug("getBuffer finished")
	return toWrite, nil
}

func (cs *ChunkServer) register(master string) error {
	zap.L().Debug("register started", zap.String("master_addr", master), zap.String("cs_addr", cs.addr))
	client, err := rpc.Dial("tcp", master)
	if err != nil {
		return err
	}

	vmap, err := cs.loadVersion()
	if err != nil {
		zap.L().Fatal("Load version failed")
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
		zap.L().Fatal("Registration call failed")
		return err
	}
	zap.L().Debug("register finished")
	return nil
}

func write(filename string, toWrite []byte, start int64) error {
	// apply writing data to local
	f, err := os.OpenFile(filename, os.O_WRONLY, os.ModeAppend) // index 0 always holds the value for primary
	if err != nil {
		zap.L().Fatal("Open file failed", zap.String("filename", filename))
	}
	defer f.Close()
	_, err = f.WriteAt(toWrite, start)
	if err != nil {
		zap.L().Fatal("Write to file failed")
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
		zap.L().Fatal("Server listener start failed")
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
				zap.L().Fatal("Accept request failed")
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
				zap.L().Info("No heatbeat from master.")
				err := cs.register(master)
				for err != nil {
					time.Sleep(HEARTBEAT_INTV)
					zap.L().Info("Trying reconnection")
					err = cs.register(master)
				}
				zap.L().Info("Reconnection Success")
			}
		}
	}(wg)
	wg.Wait()
	return nil
}
