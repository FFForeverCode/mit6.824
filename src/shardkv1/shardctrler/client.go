package shardctrler

import (
	// "log"
	"sync/atomic"

	"6.5840/kvsrv1/rpc"
	"6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	deposed *int32
	// You will have to modify this struct.
}

// The shard controller can use MakeClerk to make a clerk for the kvraft
// group with the servers `servers`.
func MakeClerk(clnt *tester.Clnt, servers []string, deposed *int32) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers, deposed: deposed}
	// You may add code here.
	return ck
}

func (ck *Clerk) isDeposed() bool {
	z := atomic.LoadInt32(ck.deposed)
	return z == 1
}

// You can reuse your kvraft Get
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{}
	args.Key = key

	// You'll have to add code here.
	return "", 0, ""
}

// You can reuse your kvraft Put
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{}
	args.Key = key
	args.Value = value
	args.Version = version

	// You'll have to add code here.
	return ""
}
