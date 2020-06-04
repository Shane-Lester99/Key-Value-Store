package pbservice

import (
	"hash/fnv"
	"time"
)

// Time interval constants

// clients should send a Ping RPC this often,
// to tell the viewservice that the client is alive.
const PingInterval = time.Millisecond * 100

// the viewserver will declare a client dead if it misses
// this many Ping RPCs in a row.
const DeadPings = 5

// Err constants
const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

// Method constants

const (
	Put           = "Put"
	Get           = "Get"
	PutHash       = "PutHash"
	DbUpdate      = "DbUpdate"
	PrevReqUpdate = "PrevReqUpdate"
	InitBackup    = "InitBackup"
)

type Method string

type PutArgs struct {
	Key     string
	Value   string
	DoHash  bool // For PutHash
	ReqType Method
	ReqNum  int64
	Sender  string
}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash
}

type GetArgs struct {
	Key     string
	ReqType Method
	ReqNum  int64
	Sender  string
}

type GetReply struct {
	Err   Err
	Value string
}

type ForwardArgs struct {
	ReqType  Method
	Database map[string]string
	PrevReq  map[string]*ReqCard
	Key      string
}

type ForwardReply struct {
	Err      Err
	Database map[string]string
	PrevReq  map[string]*ReqCard
}

type ReqCard struct {
	GArgs      *GetArgs
	GReply     *GetReply
	PArgs      *PutArgs
	PReply     *PutReply
	ReqType    Method
	PrevReqKey string
	Processed  bool
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
