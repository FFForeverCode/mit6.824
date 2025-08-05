package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	PENDING     = 1
	IN_PROGRESS = 2
	DONE        = 3
)

var mutexForCoordinator = sync.Mutex{}
var isUpdating = sync.Mutex{}

var lastDoneTime = time.Now()

type Coordinator struct {
	// Your definitions here.
	nMap        int
	nReduce     int
	mapTasks    []int
	reduceTasks []int
	inputFiles  []string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) UpdateStatus(args *UpdateStatusArgs, reply *UpdateStatusReply) error {
	mutexForCoordinator.Lock()
	defer mutexForCoordinator.Unlock()
	reply.CanUpdate = true
	if args.IsMapTask {
		if c.mapTasks[args.Index] == DONE {
			reply.CanUpdate = false
		} else {
			c.mapTasks[args.Index] = args.Status
			if args.Status == DONE {
				lastDoneTime = time.Now()
			}
		}
	} else {
		if c.reduceTasks[args.Index] == DONE {
			reply.CanUpdate = false
		} else {
			fmt.Println("update reduce task status: ", args.Index, args.Status)
			c.reduceTasks[args.Index] = args.Status
			if args.Status == DONE {
				lastDoneTime = time.Now()
			}
		}
	}
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	reply.TaskIndex = -1
	isDoneMapping := true
	fmt.Println("c.reduceTasks: ", c.reduceTasks)
	for index, task := range c.mapTasks {
		mutexForCoordinator.Lock()
		if task == PENDING {
			reply.TaskIndex = index
			c.mapTasks[index] = IN_PROGRESS
			reply.IsMapTask = true
			reply.FileName = c.inputFiles[index]
			reply.NReduce = c.nReduce
			mutexForCoordinator.Unlock()
			return nil
		}
		if task != DONE {
			isDoneMapping = false
		}
		mutexForCoordinator.Unlock()
	}
	if !isDoneMapping {
		return nil
	}
	for index, task := range c.reduceTasks {
		mutexForCoordinator.Lock()
		if task == PENDING {
			reply.TaskIndex = index
			c.reduceTasks[index] = IN_PROGRESS
			reply.IsMapTask = false
			reply.NReduce = c.nReduce
			mutexForCoordinator.Unlock()
			return nil
		}
		mutexForCoordinator.Unlock()
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
	ret := true

	// Your code here.
	for _, task := range c.reduceTasks {
		if task != DONE {
			ret = false
			break
		}
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nMap = len(files)
	c.nReduce = nReduce
	c.mapTasks = make([]int, c.nMap)
	for i, _ := range c.mapTasks {
		c.mapTasks[i] = PENDING
	}
	c.reduceTasks = make([]int, c.nReduce)
	for i, _ := range c.reduceTasks {
		c.reduceTasks[i] = PENDING
	}
	c.inputFiles = files
	go c.ensureTaskProcessing()
	c.server()
	return &c
}

func (c *Coordinator) ensureTaskProcessing() {
	for {
		if lastDoneTime.Add(30 * time.Second).Before(time.Now()) {
			for index, _ := range c.mapTasks {
				isUpdating.Lock()
				if c.mapTasks[index] == IN_PROGRESS {
					c.mapTasks[index] = PENDING
				}
				isUpdating.Unlock()
			}
			for index, _ := range c.reduceTasks {
				isUpdating.Lock()
				if c.reduceTasks[index] == IN_PROGRESS {
					c.reduceTasks[index] = PENDING
				}
				isUpdating.Unlock()
			}
		}
		time.Sleep(10 * time.Second)
	}
}
