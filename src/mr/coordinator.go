package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	AllocateWorkerNO int
	NextMap          int
	IsMapDone        bool
	Worker2task      map[int]int //map task 就是Files的下标，reduce task 就是ReduceTask 的下标
	ReduceTask       []bool
	Files            []string
	Mu               sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Rpc(args *RpcArgs, reply *RpcReply) error {
	//这里分配任务
	fmt.Printf("here is coordinator running\n")
	//分配worker编号
	if args.WorkerNO == -1 {
		c.Mu.Lock()
		reply.WorkerNo = c.AllocateWorkerNO + 1
		c.AllocateWorkerNO++
		c.Mu.Unlock()
	} else {
		reply.WorkerNo = args.WorkerNO
	}
	reply.Nreduce = len(c.ReduceTask) - 1
	//做map任务
	if !c.IsMapDone {
		if c.NextMap >= len(c.Files) {
			reply.IsAllocateTask = false
		} else {
			c.Mu.Lock()
			reply.File = c.Files[c.NextMap]
			reply.TaskNo = c.NextMap
			reply.IsAllocateTask = true
			reply.IsMap = true
			c.Worker2task[reply.WorkerNo] = c.NextMap
			c.NextMap++
			c.Mu.Unlock()
		}
	} else {

	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.AllocateWorkerNO = 0
	c.NextMap = 0
	c.Worker2task = make(map[int]int)
	c.ReduceTask = make([]bool, nReduce+1)
	c.Files = files

	c.server()
	return &c
}
