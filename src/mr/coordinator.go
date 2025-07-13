package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

const IDLE byte = 0
const IN_PROGRESS byte = 1
const COMPLETED byte = 2

type Coordinator struct {
	// Your definitions here.
	MapNum        int
	ReduceNum     int
	MapTaskMap    map[int]Task
	ReduceTaskMap map[int]map[int]Task
}

type Task struct {
	Status       byte
	WorkerId     int
	Id           int
	IsMap        bool
	Mapf         func(string, string) []KeyValue
	Reducef      func(string, []string) string
	FileLocation string
}

func (e NoTaskError) Error() string {
	return "NoTaskError"
}

func (c *Coordinator) getTaskWithStatus(status byte) (*Task, bool) {
	for _, task := range c.MapTaskMap {
		if task.Status == status {
			return &task, true
		}
	}
	for _, arr := range c.ReduceTaskMap {
		for _, task := range arr {
			if task.Status == status {
				return &task, true
			}
		}
	}
	return nil, false
}

func (c *Coordinator) noIdleTask() bool {
	_, hasTask := c.getTaskWithStatus(IDLE)
	return !hasTask
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(_ *RecvTaskArgs, reply *RecvTaskReply) error {
	if c.noIdleTask() {
		return NoTaskError{}
	}

	taskPtr, _ := c.getTaskWithStatus(IDLE)
	reply.task = *taskPtr
	return nil
}

func (c *Coordinator) Handler(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	_, hasTask := c.getTaskWithStatus(COMPLETED)
	if !hasTask {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	fileNum := len(files)
	// 10 is the factor for mapping
	c.MapNum = fileNum * nReduce * 10
	c.ReduceNum = nReduce
	c.MapTaskMap = make(map[int]Task, c.MapNum)
	c.ReduceTaskMap = make(map[int]map[int]Task, c.MapNum)
	for k, _ := range c.ReduceTaskMap {
		c.ReduceTaskMap[k] = make(map[int]Task, c.ReduceNum)
	}

	c.server()
	return &c
}
