package mr

import (
	"container/list"
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Task struct {
	TaskType string
	FilePath string
	ReduceId int
}

type WorkerInfo struct {
	Id            string
	Status        string
	Task          Task
	LastAliveTime int64
}

type Coordinator struct {
	Workers           map[string]*WorkerInfo
	WorkerCount       int
	MapTasks          list.List
	ReduceTasks       list.List
	IntermediateFiles map[string]bool
	ResultFiles       []string
	Files             []string
	nReduce           int
	workerListMutex   sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) WorkerStatusUpdate(args *StatusUpdateArgs, reply *TaskReply) error {
	c.workerListMutex.Lock()
	defer c.workerListMutex.Unlock()
	log.Printf("收到状态更新: %+v\n", args)
	// log.Printf("当前状态: %+v\n", c)
	var worker = c.Workers[args.Id]
	if worker == nil {
		if args.Status != "register" {
			return errors.New("unregister operation")
		} else {
			if args.Id != "" {
				return errors.New("register should not have Id")
			}
			c.WorkerCount += 1
			reply.Id = strconv.Itoa(c.WorkerCount)
			args.Id = reply.Id
			c.Workers[reply.Id] = &WorkerInfo{reply.Id, "idle", Task{"", "", -1}, time.Now().Unix()}
			worker = c.Workers[reply.Id]
		}
	}
	worker.LastAliveTime = time.Now().Unix()
	reply.NReduce = c.nReduce
	if args.Status == "finish" {
		if args.ResultFilePath != nil {
			if worker.Task.TaskType == "map" {
				for _, f := range args.ResultFilePath {
					c.IntermediateFiles[f] = true
				}
			}
			if worker.Task.TaskType == "reduce" {
				c.ResultFiles = append(c.ResultFiles, args.ResultFilePath...)
			}
		}
		worker.Status = "idle"
		reply.Id = worker.Id
		reply.Type = "nil"
		log.Printf("任务{%+v}完成, 剩余任务数量：Map:%d Reduce:%d\n", worker.Task, c.MapTasks.Len(), c.ReduceTasks.Len())
	}

	if c.MapTasks.Len() == 0 {
		if c.ReduceTasks.Len() == 0 {
			// 所有任务完成
			return nil
		}

		// 要等到所有的Worker把Map任务完成之后再进行
		if !c.allMapTaskDone() {
			return nil
		}

		// 进行Reduce
		worker.Status = "running"
		worker.Task = c.ReduceTasks.Front().Value.(Task)
		c.ReduceTasks.Remove(c.ReduceTasks.Front())

		reply.Type = worker.Task.TaskType
		reply.IntermediateFiles = c.IntermediateFiles
		reply.ReduceNum = worker.Task.ReduceId
		
	} else {
		// 从队列中拿出任务进行返回
		worker.Status = "running"
		worker.Task = c.MapTasks.Front().Value.(Task)
		c.MapTasks.Remove(c.MapTasks.Front())

		reply.Type = worker.Task.TaskType
		reply.FilePath = worker.Task.FilePath
	}
	return nil
}

func (c *Coordinator) WorkerAlive(args *AliveArgs, reply *AliveReply) error {
	var worker = c.Workers[args.Id]
	worker.LastAliveTime = time.Now().Unix()
	reply.Status = "up"
	return nil
}

func (c *Coordinator) kickDeadWorker() {
	for k, v := range c.Workers {
		if time.Now().Unix()-v.LastAliveTime > 10000 {
			log.Printf("扫描到超时Worker: %s, 将其注销", v.Id)
			delete(c.Workers, k)
			c.MapTasks.PushBack(v.Task)
		}
	}
}

func (c *Coordinator) allMapTaskDone() bool {
	for _, worker := range c.Workers {
		if worker.Task.TaskType == "map" && worker.Status == "running" {
			return false
		}
	}
	return true
}

func (c *Coordinator) allWorkerIdle() bool {
	for _, worker := range c.Workers {
		if worker.Status != "idle" {
			return false
		}
	}
	return true
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
	ret := true

	// Your code here.
	c.workerListMutex.Lock()
	defer c.workerListMutex.Unlock()
	if c.MapTasks.Len() != 0  || c.ReduceTasks.Len() != 0 {
		return false
	}
	ret = c.allWorkerIdle()

	c.kickDeadWorker()

	if ret {
		log.Printf("任务完成，结果文件：%+v\n", c.ResultFiles)
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

	// Your code here.
	c.Workers = map[string]*WorkerInfo{}
	c.IntermediateFiles = map[string]bool{}
	c.nReduce = nReduce
	c.Files = files

	// 添加所有的Map任务
	for _, f := range files {
		c.MapTasks.PushBack(Task{"map", f, -1})
	}

	// 添加所有Reduce任务
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks.PushBack(Task{"reduce", "", i})
	}

	c.server()
	return &c
}
