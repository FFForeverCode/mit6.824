package shardctrler

//
// Shardctrler implemented as a clerk.
//

import (

	"sync/atomic"

	"6.5840/kvraft1"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/tester1"
)

const (
	ErrDeposed = "ErrDeposed"
)


// The query clerk must support only Query(); it is intended for use
// by shardkv clerks to read the current configuration (see
// ../client.go).
type QueryClerk struct {
	kvtest.IKVClerk
	// Your data here.
}

// Make a query clerk for controller's kvraft group to invoke just
// Query()
func MakeQueryClerk(clnt *tester.Clnt, servers []string) *QueryClerk {
	qck := &QueryClerk{
		IKVClerk: kvraft.MakeClerk(clnt, servers),
	}
	// Your code here.
	return qck
}

// Return the current configuration.  You can use Get() to retrieve
// the string representing the configuration and shardcfg.ToShardCfg
// to unmarshal the string into a ShardConfig.
func (qck *QueryClerk) Query() (*shardcfg.ShardConfig, rpc.Tversion) {
	// Your code here.
	return nil, 0
}

// ShardCtrlerClerk for the shard controller. It implements the
// methods for Init(), Join(), Leave(), etc.
type ShardCtrlerClerk struct {
	clnt    *tester.Clnt
	deposed int32 // set by Stepdown()

	// Your data here.
}

// Make a ShardCltlerClerk for the shard controller, which stores its
// state in a kvraft group.  You can call (and implement) the
// MakeClerk method in client.go to make a kvraft clerk for the kvraft
// group with the servers `servers`.
func MakeShardCtrlerClerk(clnt *tester.Clnt, servers []string) *ShardCtrlerClerk {
	sck := &ShardCtrlerClerk{clnt: clnt}
	// Your code here.
	return sck
}


// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvraft group for the controller at version 0.
// You can pick the key to name the configuration.
func (sck *ShardCtrlerClerk) Init(cfg *shardcfg.ShardConfig) rpc.Err {
	// Your code here
	return rpc.OK
}

// Add group gid. Use shardcfg.JoinBalance() to compute the new
// configuration; the supplied `srvrs` are the servers for the new
// group.  You can find the servers for existing groups in the
// configuration (which you can retrieve using Query()) and you can
// make a clerk for a group by calling shardgrp.MakeClerk(sck.clnt,
// servers), and then invoke its Freeze/InstallShard methods.
func (sck *ShardCtrlerClerk) Join(gid tester.Tgid, srvs []string) rpc.Err {
	// Your code here
	return rpc.ErrNoKey
}

// Group gid leaves. You can use shardcfg.LeaveBalance() to compute
// the new configuration.
func (sck *ShardCtrlerClerk) Leave(gid tester.Tgid) rpc.Err {
	// Your code here
	return rpc.ErrNoKey
}

// the tester calls Stepdown() to force a ctrler to step down while it
// is perhaps in the middle of a join/move. for your convenience, we
// also supply isDeposed() method to test rf.dead in long-running
// loops
func (sck *ShardCtrlerClerk) Stepdown() {
	atomic.StoreInt32(&sck.deposed, 1)
}

func (sck *ShardCtrlerClerk) isDeposed() bool {
	z := atomic.LoadInt32(&sck.deposed)
	return z == 1
}


// Return the current configuration
func (sck *ShardCtrlerClerk) Query() (*shardcfg.ShardConfig, rpc.Tversion, rpc.Err) {
	// Your code here.
	return nil, 0, ""
}

