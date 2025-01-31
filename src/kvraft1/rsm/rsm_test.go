package rsm

import (
	//"log"
	"testing"
)

// test that each server executes increments and updates its counter.
func TestBasic(t *testing.T) {
	ts := makeTest(t, -1)
	defer ts.cleanup()

	ts.Begin("Test RSM basic")
	for i := 0; i < 10; i++ {
		r := ts.one()
		if r.N != i+1 {
			ts.Fatalf("expected %d instead of %d", i, r.N)
		}
		ts.checkCounter(r.N, NSRV)
	}
}

// test that each server executes increments after disconnecting and
// reconnecting leader
func TestLeaderFailure(t *testing.T) {
	ts := makeTest(t, -1)
	defer ts.cleanup()

	r := ts.one()
	ts.checkCounter(r.N, NSRV)

	l := ts.disconnectLeader()
	r = ts.one()
	ts.checkCounter(r.N, NSRV-1)

	ts.connect(l)

	ts.checkCounter(r.N, NSRV)
}

// test snapshot and restore
func TestSnapshot(t *testing.T) {
	const N = 100

	ts := makeTest(t, 1000)
	defer ts.cleanup()

	for i := 0; i < N; i++ {
		ts.one()
	}
	ts.checkCounter(N, NSRV)

	// rsm must have made snapshots by now shutdown all servers and
	// restart them from a snapshot

	ts.g.Shutdown()
	ts.g.StartServers()

	// make restarted servers do one increment
	ts.one()

	ts.checkCounter(N+1, NSRV)
}
