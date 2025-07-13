package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type RpcArgs struct {
	WorkerNO int
}

type RpcReply struct {
	File           string
	WorkerNo       int //Worker 编号，唯一指定一个Worker
	TaskNo         int //map任务就是map任务编号，reduce任务就是reduce任务编号
	IsAllocateTask bool
	IsMap          bool
	Nreduce        int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
