package kvservice

import (
	"crypto/rand"
	"hash/fnv"
	"math/big"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

type PutArgs struct {
	Key    string
	Value  string
	DoHash bool // For PutHash
	// You'll have to add definitions here.
	// Field names should start with capital letters for RPC to work.
	RequestId string
}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash
	isPrimary     bool
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	RequestId string
}

type GetReply struct {
	Err       Err
	Value     string
	isPrimary bool
}

// Add your RPC definitions here.
// ======================================
// these two structs are used to send an RPC call to update the kvDB
type UpdateArgs struct {
	KVDB              map[string]string
	GetClientRequests map[string]GetReply
	PutClientRequests map[string]PutReply
}

type UpdateReply struct {
	Err       Err
	isPrimary bool
}

// ======================================

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
