package rsm

import (
	//"log"
	"testing"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/tester1"
)

type Test struct {
	*tester.Config
	t            *testing.T
	g            *tester.ServerGrp
	maxraftstate int
	srvs         []*rsmSrv
	leader       int
}

const (
	NSRV = 3
	NSEC = 10
)

func makeTest(t *testing.T, maxraftstate int) *Test {
	ts := &Test{
		t:            t,
		maxraftstate: maxraftstate,
		srvs:         make([]*rsmSrv, NSRV),
	}
	ts.Config = tester.MakeConfig(t, NSRV, true, maxraftstate, ts.mksrv)
	ts.g = ts.Group(tester.GRP0)
	return ts
}

func (ts *Test) cleanup() {
	ts.End()
	ts.Config.Cleanup()
	ts.CheckTimeout()
}

func (ts *Test) mksrv(ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *raft.Persister, maxraftstate int) tester.IKVServer {
	s := makeRsmSrv(ts, srv, ends, persister, false)
	ts.srvs[srv] = s
	return s
}

func (ts *Test) one() *Rep {
	// try all the servers, maybe one is the leader but give up after NSEC
	t0 := time.Now()
	for time.Since(t0).Seconds() < NSEC {
		index := ts.leader
		for range ts.srvs {
			if ts.g.IsConnected(index) {
				s := ts.srvs[index]
				if s.rsm != nil {
					err, rep := s.rsm.Submit(Inc{})
					if err == rpc.OK {
						ts.leader = index
						//log.Printf("leader = %d", ts.leader)
						return rep.(*Rep)
					}
				}
			}
			index = (index + 1) % len(ts.srvs)
		}
		time.Sleep(50 * time.Millisecond)
		//log.Printf("try again: no leader")
	}

	ts.Fatalf("one: took too long")
	return nil
}

func (ts *Test) checkCounter(v int, nsrv int) {
	to := 10 * time.Millisecond
	n := 0
	for iters := 0; iters < 30; iters++ {
		n = ts.countValue(v)
		if n >= nsrv {
			return
		}
		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
	}
	ts.Fatalf("checkCounter: only %d srvs have %v instead of %d", n, v, nsrv)
}

func (ts *Test) countValue(v int) int {
	i := 0
	for _, s := range ts.srvs {
		if s.counter == v {
			i += 1
		}
	}
	return i
}

func (ts *Test) disconnectLeader() int {
	//log.Printf("disconnect %d", ts.leader)
	ts.g.DisconnectAll(ts.leader)
	return ts.leader
}

func (ts *Test) connect(i int) {
	//log.Printf("connect %d", i)
	ts.g.ConnectOne(i)
}
