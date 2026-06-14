package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Value struct {
	value string 
	version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex
	// Your definitions here.
	kv map[string]*Value 
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.kv = make(map[string]*Value)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	v, ok := kv.kv[args.Key]
	kv.mu.Unlock()
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	} 
	reply.Err = rpc.OK
	reply.Value = v.value
	reply.Version = v.version
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	v, ok := kv.kv[args.Key] 
	if !ok {
		verMatch := args.Version == 0
		if verMatch {
			kv.kv[args.Key] = &Value{
				value: args.Value,
				version: 1,
			}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
		kv.mu.Unlock()
		return
	}
	
	verMatch := args.Version == v.version
	if verMatch {
		kv.kv[args.Key] = &Value{
			value: args.Value,
			version: args.Version + 1,
		}
		reply.Err = rpc.OK
		kv.mu.Unlock()
	} else {
		reply.Err = rpc.ErrVersion
		kv.mu.Unlock()
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}


// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
