package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

/*
*
调度节点：
1.选择map节点接受数据，进行数据处理
2.监控map节点处理状态：若超时->需要重新选择一个空闲节点进行数据处理
3.选择reduce节点进行收集【确保key相同的数据传输到同一个reduce节点】
4.监控map、reduce节点的空闲状态
*/
type Coordinator struct {
	// Your definitions here.
	nMap         int      //集群中map任务数量
	mapStatus    []int    //map任务的状态 0 没有分配 1 执行中 2 已完成
	nReduce      int      //集群中reduce任务数量
	reduceStatus []int    //reduce任务的状态
	files        []string //传输的文件
	mapDone      int
	reduceDone   int
	lock         sync.Mutex //悲观锁
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) ReceiveFinishedMap(args *WorkerArgs, reply *WorkerReply) error {
	m.mu.Lock()
	m.mapfinished++
	m.maptasklog[args.MapTaskNumber] = 2 // log the map task as finished
	m.mu.Unlock()
	return nil
}

func (m *Master) ReceiveFinishedReduce(args *WorkerArgs, reply *WorkerReply) error {
	m.mu.Lock()
	m.reducefinished++
	m.reducetasklog[args.ReduceTaskNumber] = 2 // log the reduce task as finished
	m.mu.Unlock()
	return nil
}

func (m *Master) AllocateTask(args *WorkerArgs, reply *WorkerReply) error {
	m.mu.Lock()
	if m.mapfinished < m.nMap {
		// allocate new map task
		allocate := -1
		for i := 0; i < m.nMap; i++ {
			if m.maptasklog[i] == 0 {
				allocate = i
				break
			}
		}
		if allocate == -1 {
			// waiting for unfinished map jobs
			reply.Tasktype = 2
			m.mu.Unlock()
		} else {
			// allocate map jobs
			reply.NReduce = m.nReduce
			reply.Tasktype = 0
			reply.MapTaskNumber = allocate
			reply.Filename = m.files[allocate]
			m.maptasklog[allocate] = 1 // waiting
			m.mu.Unlock()              // avoid deadlock
			go func() {
				time.Sleep(time.Duration(10) * time.Second) // wait 10 seconds
				m.mu.Lock()
				if m.maptasklog[allocate] == 1 {
					// still waiting, assume the map worker is died
					m.maptasklog[allocate] = 0
				}
				m.mu.Unlock()
			}()
		}
	} else if m.mapfinished == m.nMap && m.reducefinished < m.nReduce {
		// allocate new reduce task
		allocate := -1
		for i := 0; i < m.nReduce; i++ {
			if m.reducetasklog[i] == 0 {
				allocate = i
				break
			}
		}
		if allocate == -1 {
			// waiting for unfinished reduce jobs
			reply.Tasktype = 2
			m.mu.Unlock()
		} else {
			// allocate reduce jobs
			reply.NMap = m.nMap
			reply.Tasktype = 1
			reply.ReduceTaskNumber = allocate
			m.reducetasklog[allocate] = 1 // waiting
			m.mu.Unlock()
			go func() {
				time.Sleep(time.Duration(10) * time.Second) // wait 10 seconds
				m.mu.Lock()
				if m.reducetasklog[allocate] == 1 {
					// still waiting, assume the reduce worker is died
					m.reducetasklog[allocate] = 0
				}
				m.mu.Unlock()
			}()
		}
	} else {
		reply.Tasktype = 3
		m.mu.Unlock()
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

	c.server()
	return &c
}
