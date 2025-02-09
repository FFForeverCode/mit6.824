package rsm

import (
	"bytes"
	"log"
	"sync"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/tester1"
)

type Inc struct {
}

type Rep struct {
	N int
}

type rsmSrv struct {
	ts      *Test
	me      int
	rsm     *RSM
	mu      sync.Mutex
	counter int
}

func makeRsmSrv(ts *Test, srv int, ends []*labrpc.ClientEnd, persister *tester.Persister, snapshot bool) *rsmSrv {
	//log.Printf("mksrv %d", srv)
	labgob.Register(Op{})
	labgob.Register(Inc{})
	labgob.Register(Rep{})
	s := &rsmSrv{
		ts: ts,
		me: srv,
	}
	s.rsm = MakeRSM(ends, srv, persister, ts.maxraftstate, s)
	return s
}

func (rs *rsmSrv) DoOp(req any) any {
	//log.Printf("%d: DoOp: %v", rs.me, req)
	if _, ok := req.(Inc); ok == false {
		// wrong type! expecting an Inc.
		log.Fatalf("DoOp called with the wrong type")
	}
	rs.mu.Lock()
	rs.counter += 1
	rs.mu.Unlock()
	return &Rep{rs.counter}
}

func (rs *rsmSrv) Snapshot() []byte {
	//log.Printf("%d: snapshot", rs.me)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rs.counter)
	return w.Bytes()
}

func (rs *rsmSrv) Restore(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rs.counter) != nil {
		log.Fatalf("%v couldn't decode counter", rs.me)
	}
	//log.Printf("%d: restore %d", rs.me, rs.counter)
}

func (rs *rsmSrv) Kill() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	//log.Printf("kill %d", rs.me)
	//rs.rsm.Kill()
	rs.rsm = nil
}

func (rs *rsmSrv) Raft() *raft.Raft {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.rsm.Raft()
}
