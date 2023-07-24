package main

import (
	"sync"
	"time"
)

// SERVER INTERNAL =============================================
// chunkserver, masterserver defined here for start.go

type chunkMeta struct {
	handle  handle
	servers []string
	version int
	primary string    // address of primary
	lease   time.Time // time when the lease ends
}

type MasterServer struct {
	addr         string
	chunkMapping map[string][]chunkMeta // filename : list of ChunkHandle
	mappingLock  sync.RWMutex
	metaDir      string // path where master saves operation log and persistent metadata
}

type ChunkServer struct {
	dataDir        string
	addr           string
	cache          *bufferCache
	extensionBatch []handle
	lastHeatBeat   time.Time
}

type handle uint64

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
	ChunkIndex int
}

type GetChunkReturn struct {
	Handle       handle
	Chunkservers []string
	Expire       time.Time
}

type ReadArgs struct {
	expire time.Time
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
	expire          time.Time
	Replicas        []string
	AppendBufferIDs []bufferID
}

type PrimaryApplyAppendReturn int // offset

type PrimaryApplyWriteArg struct {
	expire         time.Time
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

type HeartBeatReturn struct {
	Version     int
	LeaseExtend bool
}

type CreateArgs string // absolute path

type CreateReturn int // placeholder

type NewChunkArgs handle

type NewChunkReturn int // placeholder

type DelArgs string // absolute path

type DelReturn int // placeholder

type DelChunkArgs handle

type DelChunkReturn int // placeholder
