package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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
type StatusUpdateArgs struct {
	Id string
	Status string
	ResultFilePath []string
}

type TaskReply struct {
	Id string
	// 任务类型：map\reduce\nil
	Type string
	FilePath string
	NReduce int
	ReduceNum int
	IntermediateFiles map[string]bool
}

type AliveArgs struct {
	Id string
}

type AliveReply struct {
	Status string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
