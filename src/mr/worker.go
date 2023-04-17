package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doMap(task *TaskInfo, mapf func(string, string) []KeyValue) {
	// fmt.Printf("Map id:%v, map time:%v\n", task.FileIdx, task.StartTime)
	intermediate := []KeyValue{}
	filename := task.FilePath
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	//生成临时文件，避免程序崩溃时导致错误发生
	nReduce := task.NReduce
	outFiles := make([]*os.File, nReduce)
	fileEncs := make([]*json.Encoder, nReduce)
	for outIdx := 0; outIdx < nReduce; outIdx++ {
		outFiles[outIdx], _ = ioutil.TempFile("", "mr-tmp-*")
		fileEncs[outIdx] = json.NewEncoder(outFiles[outIdx])
	}

	for _, kv := range intermediate {
		hashIdx := ihash(kv.Key) % nReduce
		enc := fileEncs[hashIdx]
		err := enc.Encode(&kv)
		if err != nil {
			fmt.Printf("kv encode failed! Error: %v\n", err)
		}
	}

	for outIdx, file := range outFiles {
		newName := "mr-" + strconv.Itoa(task.FileIdx) + "-" + strconv.Itoa(outIdx)
		oldName := filepath.Join(file.Name())
		os.Rename(oldName, newName)
		file.Close()
	}

	CallFinReport(*task)
}

func doReduce(task *TaskInfo, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for idx := 0; idx < task.FileN; idx++ {
		fileName := "mr-" + strconv.Itoa(idx) + "-" + strconv.Itoa(task.Ridx)
		file, err := os.Open(fileName)
		if err != nil {
			fmt.Printf("file:%v read failed!\n", fileName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	outFile, err := ioutil.TempFile("", "mr-tmp-out-*")
	if err != nil {
		fmt.Println("create temp file failed!")
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	newName := "mr-out-" + strconv.Itoa(task.Ridx)
	os.Rename(outFile.Name(), newName)
	outFile.Close()
	CallFinReport(*task)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		curTask := CallGetTask()
		switch curTask.TaskType {
		case "Map":
			doMap(&curTask, mapf)
		case "Reduce":
			doReduce(&curTask, reducef)
		case "Wait":
			time.Sleep(time.Second)
		case "None":
			return
		default:
			fmt.Printf("reply.type %v, wait the Map finished\n", curTask.TaskType)
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

func CallGetTask() TaskInfo {
	args := 0
	reply := TaskInfo{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		fmt.Printf("reply.type %v\n", reply.TaskType)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

func CallFinReport(task TaskInfo) {
	args := task
	reply := 0
	ok := call("Coordinator.FinReport", &args, &reply)
	if ok {
		fmt.Printf("CallFinReport success\n")
	} else {
		fmt.Printf("CallFinReport failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
