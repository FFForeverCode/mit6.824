package mr

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
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

type ByKey []KeyValue

var mutex = sync.Mutex{}

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	for {
		var reply GetTaskReply
		mutex.Lock()
		if !call("Coordinator.GetTask", &GetTaskArgs{}, &reply) {
			mutex.Unlock()
			break
		}
		if reply.TaskIndex == -1 {
			mutex.Unlock()
			time.Sleep(1 * time.Second)
			continue
		}
		if reply.IsMapTask {
			call("Coordinator.UpdateStatus", &UpdateStatusArgs{
				Status:    IN_PROGRESS,
				Index:     reply.TaskIndex,
				IsMapTask: true,
			}, &UpdateStatusReply{})
			mutex.Unlock()
			DoMapTask(mapf, reply)
		} else {
			call("Coordinator.UpdateStatus", &UpdateStatusArgs{
				Status:    IN_PROGRESS,
				Index:     reply.TaskIndex,
				IsMapTask: false,
			}, &UpdateStatusReply{})
			mutex.Unlock()
			DoReduceTask(reducef, reply)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func DoMapTask(mapf func(string, string) []KeyValue, reply GetTaskReply) {
	inputFileName := reply.FileName
	file, _ := os.Open(inputFileName)
	content := make([]byte, 1000000000)
	file.Read(content)
	file.Close()
	contentStr := string(content)
	resultKv := mapf(inputFileName, contentStr)
	for _, kv := range resultKv {
		intermediateFileName := "mr-" + strconv.Itoa(reply.TaskIndex) + "-" + strconv.Itoa(ihash(kv.Key)%reply.NReduce)
		intermediateFile, _ := os.OpenFile(intermediateFileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
		enc := json.NewEncoder(intermediateFile)
		enc.Encode(&kv)
		intermediateFile.Close()
	}
	call("Coordinator.UpdateStatus", &UpdateStatusArgs{
		Status:    DONE,
		Index:     reply.TaskIndex,
		IsMapTask: true,
	}, &UpdateStatusReply{})
}

func DoReduceTask(reducef func(string, []string) string, reply GetTaskReply) {
	kva := make([]KeyValue, 0)
	for i := 0; i < reply.NReduce; i++ {
		intermediateFile, _ := os.Open("mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.TaskIndex))
		dec := json.NewDecoder(intermediateFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		intermediateFile.Close()
	}
	sort.Sort(ByKey(kva))
	kvMap := make(map[string][]string)
	for _, kv := range kva {
		kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
	}
	lastK := ""
	for _, kv := range kva {
		k := kv.Key
		if lastK == k {
			continue
		}
		lastK = k
		vArr := kvMap[k]
		result := reducef(k, vArr)
		fileLine := k + " " + result + "\n"
		file, _ := os.OpenFile("mr-"+"out-"+strconv.Itoa(reply.TaskIndex), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
		file.Write([]byte(fileLine))
		file.Close()
	}
	call("Coordinator.UpdateStatus", &UpdateStatusArgs{
		Status:    DONE,
		Index:     reply.TaskIndex,
		IsMapTask: false,
	}, &UpdateStatusReply{})
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
