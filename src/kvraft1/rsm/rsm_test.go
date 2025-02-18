package rsm

import (
	//"log"
	"sync"
	"testing"
	"time"

	"6.5840/kvsrv1/rpc"
)

// test that each server executes increments and updates its counter.
func TestBasic4A(t *testing.T) {
	const NINC = 10
	ts := makeTest(t, -1)
	defer ts.cleanup()

	ts.Begin("Test RSM basic")
	for i := 0; i < NINC; i++ {
		r := ts.oneInc()
		if r.N != i+1 {
			ts.t.Fatalf("expected %d instead of %d", i, r.N)
		}
		ts.checkCounter(r.N, NSRV)
	}
}

// test submitting concurrently
func TestConcurrent4A(t *testing.T) {
	const NINC = 50
	ts := makeTest(t, -1)
	defer ts.cleanup()

	ts.Begin("Test concurrent submit")

	var wg sync.WaitGroup
	for i := 0; i < NINC; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ts.oneInc()
		}()
	}
	wg.Wait()
	ts.checkCounter(NINC, NSRV)
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

// test that partitioned leader doesn't commit an operation
func TestLeaderPartition4A(t *testing.T) {
	const NSUBMIT = 100

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

	done := make(chan struct{})
	go func() {
		// Submit many Dec's concurrently, which will results in many
		// raft.Start()'s, but none should commit and be executed.
		var wg sync.WaitGroup
		for i := 0; i < NSUBMIT; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				if err, rep := ts.srvs[l].rsm.Submit(Dec{}); err == rpc.OK {
					t.Fatalf("Submit %d in minority completed %v", i, rep)
				}
			}(i)
		}
		wg.Wait()
		done <- struct{}{}
	}()

	// give old leader some time to submit
	time.Sleep(10 * time.Millisecond)

	// submit an Inc in the majority
	rep := ts.oneIncPartition(p1)

	select {
	case err := <-done:
		ts.Fatalf("Dec's in minority completed %v", err)
	case <-time.After(time.Second):
	}

	// reconnect leader
	ts.connect(l)

	select {
	case <-done:
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
