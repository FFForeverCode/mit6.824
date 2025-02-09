package rsm

import (
	//"log"
	"testing"
	"time"

	"6.5840/kvsrv1/rpc"
)

// test that each server executes increments and updates its counter.
func TestBasic4A(t *testing.T) {
	ts := makeTest(t, -1)
	defer ts.cleanup()

	ts.Begin("Test RSM basic")
	for i := 0; i < 10; i++ {
		r := ts.oneInc()
		if r.N != i+1 {
			ts.t.Fatalf("expected %d instead of %d", i, r.N)
		}
		ts.checkCounter(r.N, NSRV)
	}
}

// test that each server executes increments after disconnecting and
// reconnecting leader
func TestLeaderFailure4A(t *testing.T) {
	ts := makeTest(t, -1)
	defer ts.cleanup()

	ts.Begin("Test Leader Failure")

	r := ts.oneInc()
	ts.checkCounter(r.N, NSRV)

	l := ts.disconnectLeader()
	r = ts.oneInc()
	ts.checkCounter(r.N, NSRV-1)

	ts.connect(l)

	ts.checkCounter(r.N, NSRV)
}

func TestLeaderPartition4A(t *testing.T) {
	ts := makeTest(t, -1)
	defer ts.cleanup()

	ts.Begin("Test Leader Partition")

	// submit an Inc
	r := ts.oneInc()
	ts.checkCounter(r.N, NSRV)

	// partition leader
	_, l := Leader(ts.Config, Gid)
	p1, p2 := ts.Group(Gid).MakePartition(l)
	ts.Group(Gid).Partition(p1, p2)

	done := make(chan rpc.Err)
	go func() {
		err, _ := ts.srvs[l].rsm.Submit(Inc{})
		done <- err
	}()

	// submit an Inc in the majority
	rep := ts.oneIncPartition(p1)

	select {
	case ver := <-done:
		ts.Fatalf("Inc in minority completed %v", ver)
	case <-time.After(time.Second):
	}

	// reconnect leader
	ts.connect(l)

	select {
	case err := <-done:
		if err == rpc.OK {
			ts.Fatalf("Inc in minority didn't fail")
		}
	case <-time.After(time.Second):
		ts.Fatalf("Submit after healing didn't return")
	}

	// check that all replicas have the same value for counter
	ts.checkCounter(rep.N, NSRV)
}

// test snapshot and restore
func TestSnapshot4C(t *testing.T) {
	const (
		N            = 100
		MAXRAFTSTATE = 1000
	)

	ts := makeTest(t, MAXRAFTSTATE)
	defer ts.cleanup()

	for i := 0; i < N; i++ {
		ts.oneInc()
	}
	ts.checkCounter(N, NSRV)

	sz := ts.Group(Gid).LogSize()
	if sz > 2*MAXRAFTSTATE {
		ts.Fatalf("logs were not trimmed (%v > 2 * %v)", sz, ts.maxraftstate)
	}

	// rsm must have made snapshots by now; shutdown all servers and
	// restart them from a snapshot

	ts.g.Shutdown()
	ts.g.StartServers()

	// make restarted servers do one increment
	ts.oneInc()

	ts.checkCounter(N+1, NSRV)
}
