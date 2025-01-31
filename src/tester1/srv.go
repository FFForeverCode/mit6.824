package tester

import (
	// "log"

	"6.5840/labrpc"
	"6.5840/raft"
)

type Server struct {
	net      *labrpc.Network
	saved    *raft.Persister
	kvsrv    IKVServer
	endNames []string
	clntEnds []*labrpc.ClientEnd
}

func makeServer(net *labrpc.Network, gid Tgid, nsrv int) *Server {
	srv := &Server{net: net}
	srv.endNames = make([]string, nsrv)
	srv.clntEnds = make([]*labrpc.ClientEnd, nsrv)
	for j := 0; j < nsrv; j++ {
		// a fresh set of ClientEnds.
		srv.endNames[j] = Randstring(20)
		// a fresh set of ClientEnds.
		srv.clntEnds[j] = net.MakeEnd(srv.endNames[j])
		net.Connect(srv.endNames[j], ServerName(gid, j))
	}
	return srv
}

// If restart servers, first call ShutdownServer
func (s *Server) startServer(gid Tgid) *Server {
	srv := makeServer(s.net, gid, len(s.endNames))
	// a fresh persister, so old instance doesn't overwrite
	// new instance's persisted state.
	// give the fresh persister a copy of the old persister's
	// state, so that the spec is that we pass StartKVServer()
	// the last persisted state.
	if s.saved != nil {
		srv.saved = s.saved.Copy()
	} else {
		srv.saved = raft.MakePersister()
	}
	return srv
}

func (s *Server) connect(to []int) {
	for j := 0; j < len(to); j++ {
		endname := s.endNames[to[j]]
		s.net.Enable(endname, true)
	}
}

func (s *Server) disconnect(from []int) {
	if s.endNames == nil {
		return
	}
	for j := 0; j < len(from); j++ {
		endname := s.endNames[from[j]]
		s.net.Enable(endname, false)
	}
}

// XXX lock s?
func (s *Server) shutdownServer() {
	// a fresh persister, in case old instance
	// continues to update the Persister.
	// but copy old persister's content so that we always
	// pass Make() the last persisted state.
	if s.saved != nil {
		s.saved = s.saved.Copy()
	}

	kv := s.kvsrv
	if kv != nil {
		kv.Kill()
		s.kvsrv = nil
	}
}
