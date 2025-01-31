package shardgrp

import (
	"sync/atomic"


	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardkv1/shardgrp/shardrpc"
)


type KVServer struct {
	gid  tester.Tgid
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

}


func (kv *KVServer) DoOp(req any) any {
	// Your code here
	return nil
}


func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *shardrpc.GetArgs, reply *rpc.GetReply) {
	// Your code here
}

func (kv *KVServer) Put(args *shardrpc.PutArgs, reply *rpc.PutReply) {
	// Your code here
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) Freeze(args *shardrpc.FreezeArgs, reply *shardrpc.FreezeReply) {
	// Your code here
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

// Return kv's raft struct
func (kv *KVServer) Raft() *raft.Raft {
	return kv.rsm.Raft()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *raft.Persister, maxraftstate int) tester.IKVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(shardrpc.PutArgs{})
	labgob.Register(shardrpc.GetArgs{})
	labgob.Register(shardrpc.FreezeArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here
	return kv
}
