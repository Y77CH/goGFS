package main

import (
	"bufio"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const HEARTBEAT_INTV = time.Second * 1
const LEASE_DURATION = time.Second * 10
const REP_FACTOR = 2

type persistMeta struct {
	Handle  handle // fields exported due to use of Marshal
	Version int
}

type persistMap map[string][]persistMeta

// GetChunkHandleAndLocations returns chunk handle and locations of it
func (ms *MasterServer) GetChunkHandleAndLocations(args GetChunkArgs, chunkReturn *GetChunkReturn) error {
	// pick the correct chunk
	var meta chunkMeta
	// when chunkindex is -1, the last chunk index is used
	if args.ChunkIndex == -1 {
		metas := ms.chunkMapping[args.FileName]
		meta = metas[len(metas)-1]
	} else {
		if args.ChunkIndex == len(ms.chunkMapping[args.FileName]) {
			// when chunk index requested is larger, create new chunk
			ms.createChunk(args.FileName)
		}
		meta = ms.chunkMapping[args.FileName][args.ChunkIndex]
	}
	chunkReturn.Handle = meta.handle
	chunkReturn.Chunkservers = meta.servers
	// regrants lease if expired
	if meta.lease.Before(time.Now()) {
		meta.lease = time.Now().Add(LEASE_DURATION)
	}
	chunkReturn.Expire = meta.lease
	return nil
}

// Chunkservers call this via RPC to register liveness and chunks
func (ms *MasterServer) Register(args RegisterArgs, reply *RegisterReturn) error {
	if len(args.Handles) == 0 {
		// idle chunkserver -> store in special entry (empty string, empty handle)
		ms.chunkMapping[""][0].servers = append(ms.chunkMapping[""][0].servers, args.ChunkserverAddr)
	} else {
		for h := range args.Handles { // iterate through reported handles
			for f, _ := range ms.chunkMapping { // iterate through file names
				for m := range ms.chunkMapping[f] { // iterate through metadata of each chunk
					if ms.chunkMapping[f][m].handle == h {
						// check version
						// if m.version < ver {
						// 	m.version = ver
						// 	m.servers = []string{} // version out of date
						// }

						// add chunkserver
						fmt.Printf("INFO: New chunkserver registered for handle %d\n", ms.chunkMapping[f][m].handle)
						ms.chunkMapping[f][m].servers = append(ms.chunkMapping[f][m].servers, args.ChunkserverAddr)
					}
				}
			}
			// TODO what if not exist?
		}
	}
	return nil
}

// generateHandle creates a new unique handle for a file
func (ms *MasterServer) generateHandle() handle {
	// TODO check duplicacy
	return handle(rand.Uint64())
}

// Create creates a new file with specified path, adds this path into the chunk mapping
func (ms *MasterServer) Create(args CreateArgs, ret *CreateReturn) error {
	// Check if the file already exists
	if _, ok := ms.chunkMapping[string(args)]; ok {
		return errors.New("file already exists")
	}

	// Generate a new unique handle
	newHandle := ms.generateHandle()

	// Initialize the file with no chunks
	ms.chunkMapping[string(args)] = []chunkMeta{{
		handle: newHandle,
	}}

	ms.placeChunk(string(args), newHandle)

	ms.logOperation("NMSP_ADD", string(args), 0)
	ms.logOperation("FILE_ADD", string(args), newHandle)

	return nil
}

func (ms *MasterServer) placeChunk(fileName string, newHandle handle) error {
	// Collect the server counts
	serverCounts := make(map[string]int)
	for fname, chunkList := range ms.chunkMapping {
		if fname == "" {
			// special idle entry
			for _, meta := range chunkList {
				for _, server := range meta.servers {
					serverCounts[server] = 0
				}
			}
			continue
		}
		// TODO comments
		for _, meta := range chunkList {
			for _, server := range meta.servers {
				// create and set to 0 if exist; otherwise, increment by 1
				if _, ok := ms.chunkMapping[fileName]; ok {
					serverCounts[server]++
				} else {
					serverCounts[server] = 0
				}
			}
		}
	}

	// Sort the counts to find ones with lowest load
	type serverCount struct {
		serverAddr string
		count      int
	}
	var serverCountSlice []serverCount
	for a, v := range serverCounts {
		serverCountSlice = append(serverCountSlice, serverCount{a, v})
	}
	sort.Slice(serverCountSlice, func(i, j int) bool {
		return serverCountSlice[i].count < serverCountSlice[j].count
	})

	// Create new chunks
	if len(serverCountSlice) > REP_FACTOR {
		// enough servers -> use the first given number of servers
		// otherwise, use all available
		serverCountSlice = serverCountSlice[:REP_FACTOR]
	}
	for _, p := range serverCountSlice {
		arg := NewChunkArgs(newHandle)
		ret := NewChunkReturn(0)
		client, err := rpc.Dial("tcp", p.serverAddr)
		if err != nil {
			fmt.Println("ERROR: Dial failed")
			return err
		}
		err = client.Call("ChunkServer.NewChunk", arg, &ret)
		if err != nil {
			fmt.Println("ERROR: New Chunk RPC failed")
			return err
		}

		// add server to the chunk metadata
		for i := range ms.chunkMapping[fileName] {
			if ms.chunkMapping[fileName][i].handle == newHandle {
				ms.chunkMapping[fileName][i].servers = append(ms.chunkMapping[fileName][i].servers, p.serverAddr)
			}
		}

		// remove server from the special idle entry if exist
		for i, s := range ms.chunkMapping[""][0].servers {
			if s == p.serverAddr {
				l := len(ms.chunkMapping[""][0].servers)
				ms.chunkMapping[""][0].servers[i] = ms.chunkMapping[""][0].servers[l-1]
				ms.chunkMapping[""][0].servers = ms.chunkMapping[""][0].servers[:l-1]
				break
			}
		}
	}
	return nil
}

func (ms *MasterServer) Delete(args DelArgs, ret *DelReturn) error {
	for _, meta := range ms.chunkMapping[string(args)] {
		arg := DelChunkArgs(meta.handle)
		ret := DelChunkReturn(0)
		for _, server := range meta.servers {
			client, err := rpc.Dial("tcp", server)
			if err != nil {
				return err
			}
			err = client.Call("ChunkServer.DeleteChunk", arg, &ret)
			if err != nil {
				return err
			}
		}
	}

	// delete relevant entries
	delete(ms.chunkMapping, string(args))

	ms.logOperation("NMSP_DEL", string(args), 0)
	return nil
}

func (ms *MasterServer) createChunk(fileName string) error {
	i := len(ms.chunkMapping[fileName])
	ms.mappingLock.Lock()
	ms.chunkMapping[fileName] = append(ms.chunkMapping[fileName], chunkMeta{})
	newHandle := ms.generateHandle()
	ms.chunkMapping[fileName][i].handle = newHandle
	err := ms.placeChunk(fileName, newHandle)
	if err != nil {
		return err
	}
	ms.mappingLock.Unlock()
	ms.logOperation("FILE_ADD", fileName, newHandle)
	return nil
}

// Called by master to log operation.
// Available operation types: NMSP_ADD, NMSP_DEL, FILE_ADD
// h is an optional parameter that is only used in FILE_ADD. Otherwise, it will be dropped
func (ms *MasterServer) logOperation(opType string, path string, h handle) error {
	// log operation to disk
	f, err := os.OpenFile(ms.metaDir+"log", os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		return err
	}
	defer f.Close()
	toWrite := opType + " " + path
	if opType == "FILE_ADD" {
		toWrite += " "
		toWrite += fmt.Sprint(h)
	}
	toWrite += "\n"
	if _, err = f.WriteString(toWrite); err != nil {
		return err
	}
	return nil
}

// Master repeatedly calls this function to poll info from known chunkservers
func (ms *MasterServer) heartBeat() error {
	for f, _ := range ms.chunkMapping { // iterate through file names
		for m := range ms.chunkMapping[f] { // iterate through chunks
			// TODO batch RPC
			for server := range ms.chunkMapping[f][m].servers {
				// TODO Reuse RPC client
				// for each file's each chunk's each server, heatbeat
				client, err := rpc.Dial("tcp", ms.chunkMapping[f][m].servers[server])
				if err != nil {
					fmt.Printf("INFO: Server %s dead.", ms.chunkMapping[f][m].servers[server])
				}
				arg := HeartBeatArg(m)
				var ret HeartBeatReturn
				err = client.Call("ChunkServer.HeartBeat", arg, &ret)
				if err != nil {
					fmt.Println("ERROR: HeartBeat RPC call failed")
					return err
				}

				// load to memory and write to metadata file
				ms.chunkMapping[f][m].version = int(ret.Version)
			}
		}
	}

	// Write data to metadata file after each successful heatbeat
	fmt.Println("INFO: HeatBeat")
	return nil
}

// Instantiate a new master process
func startMaster(addr string, metaDir string) error {
	master := MasterServer{}
	master.chunkMapping = map[string][]chunkMeta{}
	master.addr = addr
	master.metaDir = metaDir
	master.mappingLock = sync.RWMutex{}

	// initialize idle entry
	master.chunkMapping[""] = []chunkMeta{}
	master.chunkMapping[""] = append(master.chunkMapping[""], chunkMeta{})

	// Replay operation log (create if first start)
	_, err := os.Stat(metaDir + "log")
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// log does not exist
			fmt.Println("INFO: Operation log does not exist, creating.")
			_, err = os.Create(metaDir + "log")
			if err != nil {
				fmt.Println("ERROR: Create operation log failed")
				return err
			}
		} else {
			return err
		}
	} else {
		f, err := os.Open(master.metaDir + "log")
		defer f.Close()
		if err != nil {
			return err
		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Text()
			components := strings.Fields(line)
			if components[0] == "NMSP_ADD" {
				master.chunkMapping[components[1]] = []chunkMeta{}
				continue
			} else if components[0] == "NMSP_DEL" {
				delete(master.chunkMapping, components[1])
				continue
			} else if components[0] == "FILE_ADD" {
				h, err := strconv.ParseInt(components[2], 10, 64)
				if err != nil {
					fmt.Println("ERROR: Convert logged handle to int64 failed")
					return err
				}
				master.chunkMapping[components[1]] = append(master.chunkMapping[components[1]], chunkMeta{handle: handle(h)})
				continue
			} else {
				fmt.Println("ERROR: Invalid log line")
			}
		}
	}

	// register and get listener
	rpc.Register(&master)
	listener, err := net.Listen("tcp", master.addr)
	if err != nil {
		fmt.Println("ERROR: Server listener start failed. " + err.Error())
	}

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

	// concurrently heartbeat
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		for {
			time.Sleep(HEARTBEAT_INTV)
			master.heartBeat()
		}
	}(wg)
	wg.Wait()

	return nil
}
