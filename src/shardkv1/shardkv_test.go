package shardkv

import (
	"log"
	"testing"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/tester1"
	// "6.5840/shardkv1/shardctrler"
)

const (
	NGRP = 8
)

// Setup a k/v service with 1 shardgrp (group 0) for storing the
// controller to store its state and 1 shardgrp (group 1) to store all
// shards.  Test's controller's Init() and Query(), and shardkv's
// Get/Put without reconfiguration.
func TestStaticOneShardGroup5A(t *testing.T) {
	ts := MakeTest(t, "Test (5A): one shard group ...", true, false)
	defer ts.Cleanup()

	// The tester's setupKVService() sets up a kvraft group for the
	// controller to store configurations and calls the controller's
	// Init() method to create the first configuration.
	ts.setupKVService()
	sck := ts.ShardCtrler() // get the controller clerk from tester

	// Read the initial configuration and check it
	cfg, v, err := sck.Query()
	if err != rpc.OK {
		ts.t.Fatalf("Query failed %v", err)
	}
	if v != 1 || cfg.Num != 1 || cfg.Shards[0] != shardcfg.Gid1 {
		ts.t.Fatalf("Static wrong %v %v", cfg, v)
	}
	cfg.CheckConfig(t, []tester.Tgid{shardcfg.Gid1})

	ck := ts.MakeClerk()                          // make a shardkv clerk
	ka, va := ts.SpreadPuts(ck, shardcfg.NShards) // do some puts
	n := len(ka)
	for i := 0; i < n; i++ {
		ts.CheckGet(ck, ka[i], va[i], rpc.Tversion(1)) // check the puts
	}
}

// test shardctrler's join, which adds a new group Gid2 and must move
// shards to the new group and the old group should reject Get/Puts on
// shards that moved.
func TestJoinBasic5A(t *testing.T) {
	ts := MakeTest(t, "Test (5A): a group joins...", true, false)
	defer ts.Cleanup()

	gid1 := ts.setupKVService()
	ck := ts.MakeClerk()
	ka, va := ts.SpreadPuts(ck, shardcfg.NShards)

	sck := ts.ShardCtrler()
	cfg, _, err := sck.Query()
	if err != rpc.OK {
		ts.t.Fatalf("Query: err %v", err)
	}

	gid2 := ts.newGid()
	err = ts.joinGroups(sck, []tester.Tgid{gid2})
	if err != rpc.OK {
		ts.t.Fatalf("joinGroups: err %v", err)
	}

	cfg1, _, err := sck.Query()
	if err != rpc.OK {
		ts.t.Fatalf("Query 1: err %v", err)
	}

	if cfg.Num+1 != cfg1.Num {
		ts.t.Fatalf("wrong num %d expected %d ", cfg1.Num, cfg.Num+1)
	}

	if !cfg1.IsMember(gid2) {
		ts.t.Fatalf("%d isn't a member of %v", gid2, cfg1)
	}

	// check shards at shardcfg.Gid2
	ts.checkShutdownSharding(gid1, gid2, ka, va)

	for i := 0; i < len(ka); i++ {
		ts.CheckGet(ck, ka[i], va[i], rpc.Tversion(1))
	}

	// check shards at shardcfg.Gid1
	ts.checkShutdownSharding(gid2, gid1, ka, va)

	for i := 0; i < len(ka); i++ {
		ts.CheckGet(ck, ka[i], va[i], rpc.Tversion(1))
	}
}

// test shardctrler's leave
func TestJoinLeaveBasic5A(t *testing.T) {
	ts := MakeTest(t, "Test (5A): basic groups join/leave ...", true, false)
	defer ts.Cleanup()

	gid1 := ts.setupKVService()
	ck := ts.MakeClerk()
	ka, va := ts.SpreadPuts(ck, shardcfg.NShards)

	sck := ts.ShardCtrler()
	gid2 := ts.newGid()
	err := ts.joinGroups(sck, []tester.Tgid{gid2})
	if err != rpc.OK {
		ts.t.Fatalf("joinGroups: err %v", err)
	}

	// check shards at shardcfg.Gid2
	ts.checkShutdownSharding(gid1, gid2, ka, va)

	for i := 0; i < len(ka); i++ {
		ts.CheckGet(ck, ka[i], va[i], rpc.Tversion(1))
	}

	err = sck.Leave(shardcfg.Gid1)
	if err != rpc.OK {
		ts.t.Fatalf("Leave: err %v", err)
	}
	cfg, _, err := sck.Query()
	if err != rpc.OK {
		ts.t.Fatalf("Query err %v", err)
	}
	if cfg.IsMember(shardcfg.Gid1) {
		ts.t.Fatalf("%d is a member of %v", shardcfg.Gid1, cfg)
	}

	ts.Group(shardcfg.Gid1).Shutdown()

	for i := 0; i < len(ka); i++ {
		ts.CheckGet(ck, ka[i], va[i], rpc.Tversion(1))
	}

	// bring the crashed shard/group back to life.
	ts.Group(shardcfg.Gid1).StartServers()

	// Rejoin
	sck.Join(shardcfg.Gid1, ts.Group(shardcfg.Gid1).SrvNames())

	for i := 0; i < len(ka); i++ {
		ts.CheckGet(ck, ka[i], va[i], rpc.Tversion(1))
	}

	// check shards at shardcfg.Gid2
	ts.checkShutdownSharding(gid2, gid1, ka, va)
}

// test many groups joining and leaving, reliable or unreliable
func joinLeave5A(t *testing.T, reliable bool, part string) {
	ts := MakeTest(t, "Test (5A): many groups join/leave ...", reliable, false)
	defer ts.Cleanup()

	ts.setupKVService()
	ck := ts.MakeClerk()
	ka, va := ts.SpreadPuts(ck, shardcfg.NShards)

	sck := ts.ShardCtrler()
	grps := ts.groups(NGRP)

	ts.joinGroups(sck, grps)

	ts.checkShutdownSharding(grps[0], grps[1], ka, va)

	for i := 0; i < len(ka); i++ {
		ts.CheckGet(ck, ka[i], va[i], rpc.Tversion(1))
	}

	ts.leaveGroups(sck, grps)

	for i := 0; i < len(ka); i++ {
		ts.CheckGet(ck, ka[i], va[i], rpc.Tversion(1))
	}
}

func TestManyJoinLeaveReliable5A(t *testing.T) {
	joinLeave5A(t, true, "Test (5A): many groups join/leave reliable...")
}

func TestManyJoinLeaveUnreliable5A(t *testing.T) {
	joinLeave5A(t, false, "Test (5A): many groups join/leave unreliable...")
}

// Test we can recover from complete shutdown using snapshots
func TestSnapshot5A(t *testing.T) {
	const NGRP = 3

	ts := MakeTest(t, "Test (5A): snapshots ...", true, false)
	defer ts.Cleanup()

	ts.setupKVService()
	ck := ts.MakeClerk()
	ka, va := ts.SpreadPuts(ck, shardcfg.NShards)

	sck := ts.ShardCtrler()
	grps := ts.groups(2)
	ts.joinGroups(sck, grps)

	// check shards at shardcfg.Gid2
	ts.checkShutdownSharding(grps[0], grps[1], ka, va)

	for i := 0; i < len(ka); i++ {
		ts.CheckGet(ck, ka[i], va[i], rpc.Tversion(1))
	}

	for i := tester.Tgid(0); i < NGRP; i++ {
		ts.Group(shardcfg.Gid1).Shutdown()
	}
	for i := tester.Tgid(0); i < NGRP; i++ {
		ts.Group(shardcfg.Gid1).StartServers()
	}

	for i := 0; i < len(ka); i++ {
		ts.CheckGet(ck, ka[i], va[i], rpc.Tversion(1))
	}
}

// Test linearizability with groups joining/leaving and `nclnt`
// concurrent clerks put/get's in `unreliable` net.
func concurrentClerk(t *testing.T, nclnt int, reliable bool, part string) {
	const (
		NSEC = 20
	)

	ts := MakeTest(t, part, reliable, true)
	defer ts.Cleanup()

	ts.setupKVService()

	ka := kvtest.MakeKeys(shardcfg.NShards)
	ch := make(chan []kvtest.ClntRes)

	start := time.Now()

	go func(ch chan []kvtest.ClntRes) {
		rs := ts.SpawnClientsAndWait(nclnt, NSEC*time.Second, func(me int, ck kvtest.IKVClerk, done chan struct{}) kvtest.ClntRes {
			return ts.OneClientPut(me, ck, ka, done)
		})
		ch <- rs
	}(ch)

	sck := ts.ShardCtrler()
	grps := ts.groups(NGRP)
	ts.joinGroups(sck, grps)

	ts.leaveGroups(sck, grps)

	log.Printf("time joining/leaving %v", time.Since(start))

	rsa := <-ch

	log.Printf("rsa %v", rsa)

	ts.CheckPorcupine()
}

// Test linearizability with groups joining/leaving and 1 concurrent clerks put/get's
func TestOneConcurrentClerkReliable5A(t *testing.T) {
	concurrentClerk(t, 1, true, "Test (5A): one concurrent clerk reliable...")
}

// Test linearizability with groups joining/leaving and many concurrent clerks put/get's
func TestManyConcurrentClerkReliable5A(t *testing.T) {
	const NCLNT = 10
	concurrentClerk(t, NCLNT, true, "Test (5A): many concurrent clerks reliable...")
}

// Test linearizability with groups joining/leaving and 1 concurrent clerks put/get's
func TestOneConcurrentClerkUnreliable5A(t *testing.T) {
	concurrentClerk(t, 1, false, "Test (5A): one concurrent clerk unreliable ...")
}

// Test linearizability with groups joining/leaving and many concurrent clerks put/get's
func TestManyConcurrentClerkUnreliable5A(t *testing.T) {
	const NCLNT = 10
	concurrentClerk(t, NCLNT, false, "Test (5A): many concurrent clerks unreliable...")
}

// test recovery of partitioned controlers
func TestRecoverCtrler5B(t *testing.T) {
	const (
		NPARITITON = 10
	)

	ts := MakeTest(t, "Test (5B): recover controler ...", true, false)
	defer ts.Cleanup()

	ts.setupKVService()
	ck := ts.MakeClerk()
	ka, va := ts.SpreadPuts(ck, shardcfg.NShards)

	for i := 0; i < NPARITITON; i++ {
		ts.partitionCtrler(ck, ka, va)
	}
}

