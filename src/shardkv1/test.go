package shardkv

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"

	"6.5840/kvraft1"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
)

type Test struct {
	t *testing.T
	*kvtest.Test

	sck  *shardctrler.ShardCtrlerClerk
	part string

	maxraftstate int
	mu           sync.Mutex
	ngid         tester.Tgid
}

const (
	Controler     = tester.Tgid(0) // controler uses group 0 for a kvraft group
	NSRV          = 3              // servers per group
	INTERGRPDELAY = 200            // time in ms between group changes
)

// Setup a kvraft group (group 0) for the shard controller and make
// the controller clerk.
func MakeTest(t *testing.T, part string, reliable, randomkeys bool) *Test {
	ts := &Test{
		ngid:         shardcfg.Gid1 + 1, // Gid1 is in use
		t:            t,
		maxraftstate: -1,
	}
	cfg := tester.MakeConfig(t, NSRV, reliable, ts.StartKVServerControler)
	ts.Test = kvtest.MakeTest(t, cfg, randomkeys, ts)
	ts.sck = ts.makeShardCtrlerClerk()
	ts.Begin(part)
	return ts
}

func (ts *Test) StartKVServerControler(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister) []tester.IService {
	return kvraft.StartKVServer(servers, gid, me, persister, ts.maxraftstate)
}

func (ts *Test) MakeClerk() kvtest.IKVClerk {
	clnt := ts.Config.MakeClient()
	ck := MakeClerk(clnt, ts.makeQueryClerk())
	return &kvtest.TestClerk{ck, clnt}
}

func (ts *Test) DeleteClerk(ck kvtest.IKVClerk) {
	tck := ck.(*kvtest.TestClerk)
	ts.DeleteClient(tck.Clnt)
}

func (ts *Test) ShardCtrler() *shardctrler.ShardCtrlerClerk {
	return ts.sck
}

func (ts *Test) makeShardCtrlerClerk() *shardctrler.ShardCtrlerClerk {
	ck, _ := ts.makeShardCtrlerClerkClnt()
	return ck
}

func (ts *Test) makeShardCtrlerClerkClnt() (*shardctrler.ShardCtrlerClerk, *tester.Clnt) {
	srvs := ts.Group(Controler).SrvNames()
	clnt := ts.Config.MakeClient()
	return shardctrler.MakeShardCtrlerClerk(clnt, srvs), clnt
}

func (ts *Test) makeQueryClerk() *shardctrler.QueryClerk {
	srvs := ts.Group(Controler).SrvNames()
	clnt := ts.Config.MakeClient()
	return shardctrler.MakeQueryClerk(clnt, srvs)
}

func (ts *Test) newGid() tester.Tgid {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	gid := ts.ngid
	ts.ngid += 1
	return gid
}

func (ts *Test) groups(n int) []tester.Tgid {
	grps := make([]tester.Tgid, n)
	for i := 0; i < n; i++ {
		grps[i] = ts.newGid()
	}
	return grps
}

// Set up KVServervice with one group Gid1. Gid1 should initialize
// itself to own all shards.
func (ts *Test) setupKVService() tester.Tgid {
	scfg := shardcfg.MakeShardConfig()
	ts.Config.MakeGroupStart(shardcfg.Gid1, NSRV, ts.StartKVServerShard)
	scfg.JoinBalance(map[tester.Tgid][]string{shardcfg.Gid1: ts.Group(shardcfg.Gid1).SrvNames()})
	if err := ts.sck.Init(scfg); err != rpc.OK {
		ts.t.Fatalf("Init err %v", err)
	}
	//ts.sck.AcquireLeadership()
	return shardcfg.Gid1
}

func (ts *Test) StartKVServerShard(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister) []tester.IService {
	return shardgrp.StartKVServer(servers, gid, me, persister, ts.maxraftstate)
}

func (ts *Test) joinGroups(sck *shardctrler.ShardCtrlerClerk, gids []tester.Tgid) rpc.Err {
	for i, gid := range gids {
		ts.Config.MakeGroupStart(gid, NSRV, ts.StartKVServerShard)
		if err := sck.Join(gid, ts.Group(gid).SrvNames()); err != rpc.OK {
			return err
		}
		if i < len(gids)-1 {
			time.Sleep(INTERGRPDELAY * time.Millisecond)
		}
	}
	return rpc.OK
}

func (ts *Test) leaveGroups(sck *shardctrler.ShardCtrlerClerk, gids []tester.Tgid) rpc.Err {
	for i, gid := range gids {
		if err := sck.Leave(gid); err != rpc.OK {
			return err
		}
		ts.Config.ExitGroup(gid)
		if i < len(gids)-1 {
			time.Sleep(INTERGRPDELAY * time.Millisecond)
		}
	}
	return rpc.OK
}

func (ts *Test) checkLogs(gids []tester.Tgid) {
	for _, gid := range gids {
		n := ts.Group(gid).LogSize()
		s := ts.Group(gid).SnapshotSize()
		if ts.maxraftstate >= 0 && n > 8*ts.maxraftstate {
			ts.t.Fatalf("persister.RaftStateSize() %v, but maxraftstate %v",
				n, ts.maxraftstate)
		}
		if ts.maxraftstate < 0 && s > 0 {
			ts.t.Fatalf("maxraftstate is -1, but snapshot is non-empty!")
		}

	}
}

// make sure that the data really is sharded by
// shutting down one shard and checking that some
// Get()s don't succeed.
func (ts *Test) checkShutdownSharding(down, up tester.Tgid, ka []string, va []string) {
	const NSEC = 2

	ts.Group(down).Shutdown()

	ts.checkLogs([]tester.Tgid{down, up}) // forbid snapshots

	n := len(ka)
	ch := make(chan string)
	for xi := 0; xi < n; xi++ {
		ck1 := ts.MakeClerk()
		go func(i int) {
			v, _, _ := ck1.Get(ka[i])
			if v != va[i] {
				ch <- fmt.Sprintf("Get(%v): expected:\n%v\nreceived:\n%v", ka[i], va[i], v)
			} else {
				ch <- ""
			}
		}(xi)
	}

	// wait a bit, only about half the Gets should succeed.
	ndone := 0
	done := false
	for done == false {
		select {
		case err := <-ch:
			if err != "" {
				ts.Fatalf(err)
			}
			ndone += 1
		case <-time.After(time.Second * NSEC):
			done = true
			break
		}
	}

	// log.Printf("%d completions out of %d with %d groups", ndone, n, ngrp)
	if ndone >= n {
		ts.Fatalf("expected less than %d completions with one shard dead\n", n)
	}

	// bring the crashed shard/group back to life.
	ts.Group(down).StartServers()
}

// Run one controler and then partitioned it forever after some time
// Run another cntrler that must finish the first ctrler's unfinished
// shard moves, if there are any.
func (ts *Test) partitionCtrler(ck kvtest.IKVClerk, ka, va []string) {
	const (
		MSEC = 20
		RAND = 2000 // maybe measure?
	)

	ch := make(chan tester.Tgid)

	sck, clnt := ts.makeShardCtrlerClerkClnt()
	cfg, _, err := ts.ShardCtrler().Query()
	num := cfg.Num

	go func() {
		for true {
			ngid := ts.newGid()
			//log.Printf("join %d", ngid)
			//s := time.Now()
			ch <- ngid
			err := ts.joinGroups(sck, []tester.Tgid{ngid})
			if err == rpc.OK {
				err = ts.leaveGroups(sck, []tester.Tgid{ngid})
			}
			//log.Printf("join err %v time %v", err, time.Since(s))
			if err == shardctrler.ErrDeposed {
				log.Printf("disposed")
				return
			}
			if err != rpc.OK {
				ts.t.Fatalf("join/leave err %v", err)
			}
			time.Sleep(INTERGRPDELAY * time.Millisecond)
		}
	}()

	lastgid := <-ch

	d := time.Duration(rand.Int()%RAND) * time.Millisecond
	time.Sleep(MSEC*time.Millisecond + d)

	log.Printf("disconnect sck %v", d)

	// partition sck forever
	clnt.DisconnectAll()

	// force sck to step down
	sck.Stepdown()

	// wait until sck has no more requests in the network
	time.Sleep(labrpc.MAXDELAY)

	cfg, _, err = ts.ShardCtrler().Query()
	if err != rpc.OK {
		ts.Fatalf("Query err %v", err)
	}

	recovery := false
	present := cfg.IsMember(lastgid)
	join := num == cfg.Num
	leave := num+1 == cfg.Num
	if !present && join {
		recovery = true
	}
	if present && leave {
		recovery = true
	}

	// start new controler to pick up where sck left off
	sck0, clnt0 := ts.makeShardCtrlerClerkClnt()
	if err != rpc.OK {
		ts.Fatalf("Query err %v", err)
	}

	cfg, _, err = sck0.Query()
	if recovery {
		s := "join"
		if leave {
			s = "leave"
		}
		//log.Printf("%v in progress", s)
		present = cfg.IsMember(lastgid)
		if (join && !present) || (leave && present) {
			ts.Fatalf("didn't recover %d correctly after %v", lastgid, s)
		}
	}

	if present {
		// cleanup if disconnected after join but before leave
		ts.leaveGroups(sck0, []tester.Tgid{lastgid})
	}

	for i := 0; i < len(ka); i++ {
		ts.CheckGet(ck, ka[i], va[i], rpc.Tversion(1))
	}

	ts.Config.DeleteClient(clnt)
	ts.Config.DeleteClient(clnt0)
}
