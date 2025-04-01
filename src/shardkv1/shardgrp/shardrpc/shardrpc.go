package shardrpc

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
)

// Same as Put in kvsrv1/rpc
type PutArgs struct {
	Key     string
	Value   string
	Version rpc.Tversion
}

// Same as Get in kvsrv1/rpc
type GetArgs struct {
	Key string
}

type FreezeArgs struct {
	Shard shardcfg.Tshid
	Num   shardcfg.Tnum
}

type FreezeReply struct {
	State []byte
	Num   shardcfg.Tnum
	Err   rpc.Err
}

type InstallShardArgs struct {
	Shard shardcfg.Tshid
	State []byte
	Num   shardcfg.Tnum
}

type InstallShardReply struct {
	Err rpc.Err
}

type DeleteShardArgs struct {
	Shard shardcfg.Tshid
	Num   shardcfg.Tnum
}

type DeleteShardReply struct {
	Err rpc.Err
}
