package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for isNotDone() {
		success, task, hasIdleTask := getTaskWithStatus(IDLE)
		if !success {
			continue
		}
		if !hasIdleTask {
			continue
		}
		if task.IsMap {
			doMap(mapf, task)
		} else {
			doReduce(reducef, task)
		}
		time.Sleep(200)
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doReduce(reducef func(string, []string) string, task *Task) {

}

func doMap(mapf func(string, string) []KeyValue, task *Task) {
	content := readMapTaskContent(task)
	filename := task.InputFilePath
	kva := mapf(filename, string(content))
	for _, kva := range kva {
		appendResultToReduceFileByHashing(kva, task)
	}
	signalCoordinatorDone(task)
}

// TODO: coordinator needs to update the status from input. Also upon starting needs to update the status to inprogress
func signalCoordinatorDone(task *Task) {
	args := &FinishTaskargs{Task: task}
	reply := &FinishTaskReply{}
	call("FinishTask", args, reply)
}

func appendResultToReduceFileByHashing(kva KeyValue, task *Task) {
	log.Println("Appending result to reduce: ", kva.Key, kva.Value)
	key := kva.Key
	reduceId := ihash(key) % task.ReduceNum
	reduceFilePath := task.GetIntermediateOutputFilePath(reduceId)
	file, err := os.Open(reduceFilePath)
	if err != nil {
		var ftmp, err = os.Create(reduceFilePath)
		if err != nil {
			log.Fatal(err)
		}
		file = ftmp
	}
	encoder := json.NewEncoder(file)
	jsonErr := encoder.Encode(&kva)
	if jsonErr != nil {
		log.Fatal(jsonErr)
	}
}

func read(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	if content == nil {
		return ""
	}
	return string(content)
}

func readMapTaskContent(task *Task) string {
	filename := task.InputFilePath
	content := read(filename)
	return content
}

/*
*
The return value is (success， taskptr， hasTask)
*/
func getTaskWithStatus(status byte) (bool, *Task, bool) {
	args := GetTaskWithStatusArgs{}
	args.Status = status
	reply := GetTaskWithStatusReply{}
	success := call("GetTaskWithStatusRemote", &args, &reply)
	if success {
		return true, &reply.Task, reply.HasTask
	}
	return false, nil, false
}

func isNotDone() bool {
	return false
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
