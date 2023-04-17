package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskInfo struct {
	TaskType  string //分为四种类型：Map、Reduce、Wait(等待所有Map执行完)、None(无任务，结束)
	FilePath  string
	NReduce   int
	Ridx      int //reduce 任务idx
	FileN     int //总文件数
	FileIdx   int //当前任务文件序列化
	StartTime int64
}

type TaskQue struct {
	mQue  map[TaskInfo]int
	mutex sync.Mutex
}

func (t *TaskQue) pop() TaskInfo {
	res := TaskInfo{}
	t.mutex.Lock()
	if len(t.mQue) > 0 {
		for key, _ := range t.mQue {
			res = key
			break
		}
		delete(t.mQue, res)
	}
	t.mutex.Unlock()
	res.StartTime = time.Now().Unix()
	return res
}

func (t *TaskQue) insert(task *TaskInfo) bool {
	t.mutex.Lock()
	t.mQue[*task] = 1
	t.mutex.Unlock()
	return true
}

func (t *TaskQue) getLen() int {
	length := 0
	t.mutex.Lock()
	length = len(t.mQue)
	t.mutex.Unlock()
	return length
}

func (t *TaskQue) empty() bool {
	mlen := t.getLen()
	return mlen == 0
}

func (t *TaskQue) delete(task *TaskInfo) bool {
	t.mutex.Lock()
	delete(t.mQue, *task)
	t.mutex.Unlock()
	return true
}

type Coordinator struct {
	// Your definitions here.
	mapDo       TaskQue
	mapDoing    TaskQue
	reduceDo    TaskQue
	reduceDoing TaskQue
	nReduce     int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args int, reply *TaskInfo) error {

	//map和reduce正在执行的任务超过10秒，则重新执行
	if !c.mapDoing.empty() {
		for key, _ := range c.mapDoing.mQue {
			if time.Now().Unix()-key.StartTime > 10 {
				tmp := c.mapDoing.pop()
				c.mapDo.insert(&tmp)
			}
		}
	}
	if !c.reduceDoing.empty() {
		for key, _ := range c.reduceDoing.mQue {
			if time.Now().Unix()-key.StartTime > 10 {
				tmp := c.reduceDoing.pop()
				c.reduceDo.insert(&tmp)
			}
		}
	}

	if !c.mapDo.empty() {
		*reply = c.mapDo.pop()
		c.mapDoing.insert(reply)
		return nil
	}

	if !c.mapDoing.empty() {
		*reply = TaskInfo{"Wait", "", 0, 0, 0, 0, 0}
		return nil
	}

	if !c.reduceDo.empty() {
		*reply = c.reduceDo.pop()
		c.reduceDoing.insert(reply)
		return nil
	}

	if c.Done() {
		*reply = TaskInfo{"None", "", 0, 0, 0, 0, 0}
	} else {
		*reply = TaskInfo{"Wait", "", 0, 0, 0, 0, 0}
	}

	return nil
}

func (c *Coordinator) FinReport(args TaskInfo, reply *int) error {
	if args.TaskType == "Map" {
		c.mapDoing.delete(&args)
	} else if args.TaskType == "Reduce" {
		c.reduceDoing.delete(&args)
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	if c.mapDo.empty() && c.mapDoing.empty() && c.reduceDoing.empty() && c.reduceDoing.empty() { //可能会存在bug，因为求几个队列是否为空并未一起求得
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nReduce = nReduce
	c.mapDo.mQue = make(map[TaskInfo]int)
	c.mapDoing.mQue = make(map[TaskInfo]int)
	c.reduceDo.mQue = make(map[TaskInfo]int)
	c.reduceDoing.mQue = make(map[TaskInfo]int)
	for idx := range files {
		tmp := TaskInfo{"Map", files[idx], nReduce, 0, len(files), idx, 0}
		c.mapDo.insert(&tmp)
	}
	for idx := 0; idx < nReduce; idx++ {
		tmp := TaskInfo{"Reduce", "", nReduce, idx, len(files), -1, 0}
		c.reduceDo.insert(&tmp)
	}

	c.server()
	return &c
}
