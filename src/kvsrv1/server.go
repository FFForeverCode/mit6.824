package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// Args.Version is 0.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
}

// You can ignore for this lab
func (kv *KVServer) Kill() {
}

// You can ignore for this lab
func (kv *KVServer) Raft() *raft.Raft {
	return nil
}


// You can ignore all arguments; they are for replicated KVservers in lab 4
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *raft.Persister, maxraftstate int) tester.IKVServer {
	kv := MakeKVServer()
	return kv
}
