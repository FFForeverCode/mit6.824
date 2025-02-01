package raft

import (
	"fmt"
	//log
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"6.5840/labrpc"
	"6.5840/tester1"
)

type Test struct {
	*tester.Config
	t *testing.T
	n int
	g *tester.ServerGrp

	finished int32

	mu       sync.Mutex
	srvs     []*rfsrv
	maxIndex int
	snapshot bool
}

func makeTest(t *testing.T, n int, reliable bool, snapshot bool) *Test {
	ts := &Test{
		t:        t,
		n:        n,
		srvs:     make([]*rfsrv, n),
		snapshot: snapshot,
	}
	ts.Config = tester.MakeConfig(t, n, reliable, ts.mksrv)
	ts.Config.SetLongDelays(true)
	ts.g = ts.Group(tester.GRP0)
	return ts
}

func (ts *Test) cleanup() {
	atomic.StoreInt32(&ts.finished, 1)
	ts.End()
	ts.Config.Cleanup()
	ts.CheckTimeout()
}

func (ts *Test) mksrv(ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	s := newRfsrv(ts, srv, ends, persister, ts.snapshot)
	ts.srvs[srv] = s
	return []tester.IService{s, s.raft}
}

func (ts *Test) restart(i int) {
	ts.g.StartServer(i) // which will call mksrv to make a new server
	ts.Group(tester.GRP0).ConnectAll()
}

func (ts *Test) checkOneLeader() int {
	for iters := 0; iters < 10; iters++ {
		ms := 450 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		leaders := make(map[int][]int)
		for i := 0; i < ts.n; i++ {
			if ts.g.IsConnected(i) {
				if term, leader := ts.srvs[i].GetState(); leader {
					leaders[term] = append(leaders[term], i)
				}
			}
		}

		lastTermWithLeader := -1
		for term, leaders := range leaders {
			if len(leaders) > 1 {
				ts.Fatalf("term %d has %d (>1) leaders", term, len(leaders))
			}
			if term > lastTermWithLeader {
				lastTermWithLeader = term
			}
		}

		if len(leaders) != 0 {
			return leaders[lastTermWithLeader][0]
		}
	}
	ts.Fatalf("expected one leader, got none")
	return -1
}

func (ts *Test) checkTerms() int {
	term := -1
	for i := 0; i < ts.n; i++ {
		if ts.g.IsConnected(i) {
			xterm, _ := ts.srvs[i].GetState()
			if term == -1 {
				term = xterm
			} else if term != xterm {
				ts.Fatalf("servers disagree on term")
			}
		}
	}
	return term
}

func (ts *Test) checkLogs(i int, m ApplyMsg) (string, bool) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	err_msg := ""
	v := m.Command
	me := ts.srvs[i]
	for j, rs := range ts.srvs {
		if old, oldok := rs.Logs(m.CommandIndex); oldok && old != v {
			//log.Printf("%v: log %v; server %v\n", i, me.logs, rs.logs)
			// some server has already committed a different value for this entry!
			err_msg = fmt.Sprintf("commit index=%v server=%v %v != server=%v %v",
				m.CommandIndex, i, m.Command, j, old)
		}
	}
	_, prevok := me.logs[m.CommandIndex-1]
	me.logs[m.CommandIndex] = v
	if m.CommandIndex > ts.maxIndex {
		ts.maxIndex = m.CommandIndex
	}
	return err_msg, prevok
}

// check that none of the connected servers
// thinks it is the leader.
func (ts *Test) checkNoLeader() {
	for i := 0; i < ts.n; i++ {
		if ts.g.IsConnected(i) {
			_, is_leader := ts.srvs[i].GetState()
			if is_leader {
				ts.Fatalf("expected no leader among connected servers, but %v claims to be leader", i)
			}
		}
	}
}

// how many servers think a log entry is committed?
func (ts *Test) nCommitted(index int) (int, any) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	count := 0
	var cmd any = nil
	for _, rs := range ts.srvs {
		if rs.applyErr != "" {
			ts.t.Fatal(rs.applyErr)
		}

		cmd1, ok := rs.Logs(index)

		if ok {
			if count > 0 && cmd != cmd1 {
				ts.Fatalf("committed values do not match: index %v, %v, %v",
					index, cmd, cmd1)
			}
			count += 1
			cmd = cmd1
		}
	}
	return count, cmd
}

// do a complete agreement.
// it might choose the wrong leader initially,
// and have to re-submit after giving up.
// entirely gives up after about 10 seconds.
// indirectly checks that the servers agree on the
// same value, since nCommitted() checks this,
// as do the threads that read from applyCh.
// returns index.
// if retry==true, may submit the command multiple
// times, in case a leader fails just after Start().
// if retry==false, calls Start() only once, in order
// to simplify the early Lab 3B tests.
func (ts *Test) one(cmd any, expectedServers int, retry bool) int {
	t0 := time.Now()
	starts := 0
	for time.Since(t0).Seconds() < 10 && ts.checkFinished() == false {
		// try all the servers, maybe one is the leader.
		index := -1
		for range ts.srvs {
			starts = (starts + 1) % len(ts.srvs)
			var rf *Raft
			if ts.g.IsConnected(starts) {
				rf = ts.srvs[starts].raft
			}
			if rf != nil {
				//log.Printf("peer %d Start %v", starts, cmd)
				index1, _, ok := rf.Start(cmd)
				if ok {
					index = index1
					break
				}
			}
		}

		if index != -1 {
			// somebody claimed to be the leader and to have
			// submitted our command; wait a while for agreement.
			t1 := time.Now()
			for time.Since(t1).Seconds() < 2 {
				nd, cmd1 := ts.nCommitted(index)
				if nd > 0 && nd >= expectedServers {
					// committed
					if cmd1 == cmd {
						// and it was the command we submitted.
						return index
					}
				}
				time.Sleep(20 * time.Millisecond)
			}
			if retry == false {
				ts.Fatalf("one(%v) failed to reach agreement", cmd)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if ts.checkFinished() == false {
		ts.Fatalf("one(%v) failed to reach agreement", cmd)
	}
	return -1
}

func (ts *Test) checkFinished() bool {
	z := atomic.LoadInt32(&ts.finished)
	return z != 0
}

// wait for at least n servers to commit.
// but don't wait forever.
func (ts *Test) wait(index int, n int, startTerm int) any {
	to := 10 * time.Millisecond
	for iters := 0; iters < 30; iters++ {
		nd, _ := ts.nCommitted(index)
		if nd >= n {
			break
		}
		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
		if startTerm > -1 {
			for _, rs := range ts.srvs {
				if t, _ := rs.raft.GetState(); t > startTerm {
					// someone has moved on
					// can no longer guarantee that we'll "win"
					return -1
				}
			}
		}
	}
	nd, cmd := ts.nCommitted(index)
	if nd < n {
		ts.Fatalf("only %d decided for index %d; wanted %d",
			nd, index, n)
	}
	return cmd
}
