package main

import (
	"sync"
	"time"
)

// SERVER INTERNAL =============================================

// TODO distinguish volatile vs non volatile
type chunkMeta struct {
	handle  handle
	servers []string
	version int
	primary string // address of primary
	lease   time.Time
}

type MasterServer struct {
	addr         string
	chunkMapping map[string][]chunkMeta // filename : list of ChunkHandle
	metaDir      string                 // path where master saves operation log and persistent metadata
}

type ChunkServer struct {
	dataDir string
	addr    string
	cache   *bufferCache
}

type handle int64

// {handle, timestamp} : byte
type bufferCache struct {
	lock   sync.RWMutex
	buffer map[bufferID][]byte
}

// exported due to data push rpc call
type bufferID struct {
	Handle    handle
	Timestamp time.Time
}

type chunk struct {
	handle   handle
	checksum string
}

// RPC =========================================================
// for goRPC to work, types have to be exported, so as their fields

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

type ApplyMutationArg struct {
	ChunkOffset  int64 // required by WriteAt
	DataBufferID bufferID
}

type ApplyMutationReturn int // placeholder

type HeartBeatArg handle

type HeartBeatReturn int // version number
