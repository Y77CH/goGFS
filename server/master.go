package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const HEARTBEAT_INTV = time.Second * 3

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
		meta = ms.chunkMapping[args.FileName][args.ChunkIndex]
	}
	chunkReturn.Handle = meta.handle
	chunkReturn.Chunkservers = meta.servers
	return nil
}

// Chunkservers call this via RPC to register liveness and chunks
func (ms *MasterServer) Register(args RegisterArgs, reply *RegisterReturn) error {
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
	err := ms.writeMeta()
	if err != nil {
		return err
	}
	return nil
}

func (ms *MasterServer) writeMeta() error {
	// load metadata into new data structure
	pmap := persistMap{}
	for n, m := range ms.chunkMapping {
		pmap[n] = []persistMeta{}
		for _, pm := range m {
			pmap[n] = append(pmap[n], persistMeta{pm.handle, pm.version})
		}
	}
	// convert to json
	jsonMeta, err := json.Marshal(pmap)
	if err != nil {
		return err
	}
	// write
	f, err := os.Create(ms.metaDir + "mappings.json")
	defer f.Close()
	if err != nil {
		return err
	}
	_, err = f.Write(jsonMeta)
	if err != nil {
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
				ms.chunkMapping[f][m].version = int(ret)
			}
		}
	}

	// Write data to metadata file after each successful heatbeat
	err := ms.writeMeta()
	if err != nil {
		return err
	}
	fmt.Println("INFO: HeatBeat")
	return nil
}

// Instantiate a new master process
func startMaster(addr string, metaDir string) error {
	master := MasterServer{}
	master.chunkMapping = map[string][]chunkMeta{}
	master.addr = addr
	master.metaDir = metaDir

	// Read persistent meta data
	_, err := os.Stat(metaDir + "mappings.json")
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// create mapping file if not exist
			fmt.Println("INFO: Mapping file not exist, creating.")
			_, err = os.Create(metaDir + "mappings.json")
			if err != nil {
				fmt.Println("ERROR: Create file failed")
				return err
			}
		} else {
			return err
		}
	} else {
		// load mapping if exist
		jsonVal, err := ioutil.ReadFile(metaDir + "mappings.json") // it's reasonable to read all at once due to the small size of metadata
		if err != nil {
			fmt.Println("ERROR: Read mapping failed")
			return err
		}
		var meta persistMap
		json.Unmarshal(jsonVal, &meta)
		for n, m := range meta {
			master.chunkMapping[n] = []chunkMeta{}
			for _, pm := range m {
				master.chunkMapping[n] = append(master.chunkMapping[n], chunkMeta{pm.Handle, []string{}, pm.Version, "", time.Time{}})
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
