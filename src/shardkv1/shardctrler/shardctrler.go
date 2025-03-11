package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (

	"sync/atomic"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/kvsrv1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/tester1"
)


// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()
	leases bool

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt, leases bool) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt, leases: leases}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In this method you can implement recovery (part B) and
// use a lock to become leader (part C).  InitController may fail when
// another controller supersedes (e.g., when this controller is
// partitioned during recovery).
func (sck *ShardCtrler) InitController() rpc.Err {
	return rpc.ErrNoKey
}

// The tester calls ExitController to exit a controller. In part B and
// C, release lock.
func (sck *ShardCtrler) ExitController() {
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new. It may return an error
// if this controller is disconnected for a while and another
// controller takes over in the mean time, as in part C.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) rpc.Err {
	return rpc.OK
}

// Tester "kills" shardctrler by calling Kill().  For your
// convenience, we also supply isKilled() method to test killed in
// loops.
func (sck *ShardCtrler) Kill() {
	atomic.StoreInt32(&sck.killed, 1)
}

func (sck *ShardCtrler) isKilled() bool {
	z := atomic.LoadInt32(&sck.killed)
	return z == 1
}


// Return the current configuration
func (sck *ShardCtrler) Query() (*shardcfg.ShardConfig, rpc.Tversion) {
	// Your code here.
	return nil, 0
}

