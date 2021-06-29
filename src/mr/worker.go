package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// 注册
	var taskReply, success = StatusUpdate(StatusUpdateArgs{"", "register", nil})
	var Id = taskReply.Id
	logFile, err := os.OpenFile(fmt.Sprintf("worker-%s.log", Id), os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal("创建日志文件失败")
	}
	log.SetOutput(logFile)
	for {
		log.Printf("当前任务：%+v\n", taskReply)
		// do task
		if taskReply.Type == "map" {
			resultPaths, e := DoMap(mapf, taskReply, Id)
			if e != nil {
				return
			}
			taskReply,success = StatusUpdate(StatusUpdateArgs{Id, "finish", resultPaths})
			if !success {
				log.Panic("rpc失败，认为是coordinator已退出，worker退出")
			}
		} else if taskReply.Type == "reduce" {
			resultPath,e := DoReduce(reducef, taskReply, Id)
			if e != nil {
				return
			}
			taskReply,success = StatusUpdate(StatusUpdateArgs{Id, "finish", []string{resultPath}})
			if !success {
				log.Panic("rpc失败，认为是coordinator已退出，worker退出")
			}
		} else {
			time.Sleep(time.Duration(3 * time.Second))
			taskReply, success = StatusUpdate(StatusUpdateArgs{Id, "idle", nil})
			if !success {
				log.Println("rpc失败，认为是coordinator已退出，worker退出")
				return
			}
		}

		// if Alive(AliveArgs{Id}).Status != "up" {
		// 	log.Fatalf("%s:alive未得到响应，退出...", Id)
		// 	break
		// }
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func DoReduce(reducef func(string, []string) string,
	taskReply TaskReply,
	id string) (string,error) {

	log.Println("开始DoReduce")
	valueMap := map[string][]string{}
	for filePath := range taskReply.IntermediateFiles {
		
		if filePath[strings.LastIndex(filePath, "-")+1:strings.Index(filePath, ".")] != fmt.Sprint(taskReply.ReduceNum) {
			continue
		}
		log.Printf("开始处理%s\n", filePath)
		kvs := ReadIntermdidate(filePath)
		log.Printf("读取到了%d个KeyValue\n", len(kvs))
		for _, kv := range kvs {
			valueMap[kv.Key] = append(valueMap[kv.Key], kv.Value)
		}
	}
	log.Printf("Intermediate文件读取完毕, 共得到%d个Key, 开始reduce\n", len(valueMap))
	resultPath := fmt.Sprintf("mr-out-%d", taskReply.ReduceNum)
	outputFile, outputError := os.OpenFile(resultPath, os.O_WRONLY|os.O_CREATE, 0666)
	if outputError != nil {
		fmt.Printf("An error occurred with file opening or creation\n")
		return "", outputError
	}
	defer outputFile.Close()
	outputWriter := bufio.NewWriter(outputFile)

	for key, values := range valueMap {
		v := reducef(key, values)
		_, err :=outputWriter.WriteString(fmt.Sprintf("%s %s\n", key, v))
		if err != nil {
			log.Fatalf("写入文件结果文件异常: %+v", err)
		}
	}
	outputWriter.Flush()

	return resultPath, nil
}

func DoMap(mapf func(string, string) []KeyValue,
	taskReply TaskReply,
	id string) ([]string, error) {
	log.Printf("执行DoMap，FilePath:%s\n", taskReply.FilePath)
	// 按行读取文件
	inputFile, inputError := os.Open(taskReply.FilePath)
	if inputError != nil {
		log.Fatalf("打开文件时出错: %s", inputError.Error())
		return nil, inputError
	}
	defer inputFile.Close()
	content, err := ioutil.ReadAll(inputFile)
	if err != nil {
		log.Fatalf("DoMap 读取文件失败 %v: %+v", taskReply.FilePath, err)
	}
	intermediate := mapf(taskReply.FilePath, string(content))
	log.Printf("DoMap完成，得到Key %d个", len(intermediate))
	return saveIntermediate(intermediate, id, taskReply.NReduce)
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func saveIntermediate(intermediate []KeyValue, workerId string, NReduce int) ([]string,error) {
	if len(intermediate) == 0 {
		return nil, nil
	}
	sort.Sort(ByKey(intermediate))
	key := intermediate[0].Key
	values := []string{}
	resultFileMap := map[string]bool{}
	for _, v := range intermediate {
		if key != v.Key {
			resultFile, e := writeIntermeidate(workerId, NReduce, key, values)
			if e != nil {
				return nil, e
			}
			resultFileMap[resultFile] = true
			values = []string{}
			key = v.Key
		}
		values = append(values, v.Value)
	}
	resultFile, e := writeIntermeidate(workerId, NReduce, key, values)
	if e != nil {
		return nil, e
	}
	resultFileMap[resultFile] = true
	resultFiles := []string{}
	for k := range resultFileMap {
		resultFiles = append(resultFiles, k)
	}
	return resultFiles, nil
}

func writeIntermeidate(workerId string, NReduce int, key string, values []string) (string,error) {
	filePath := fmt.Sprintf("mr-%s-%d.intermediate.tmp", workerId, ihash(key) % NReduce)
	outputFile, outputError := os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if outputError != nil {
		fmt.Printf("An error occurred with file opening or creation\n")
		return "", outputError
	}
	defer outputFile.Close()
	outputWriter := bufio.NewWriter(outputFile)
	for _, v := range values {
		outputWriter.WriteString(fmt.Sprintf("%s:%s\n", key, v))
	}
	outputWriter.Flush()
	return filePath, nil
}

func ReadIntermdidate(filePath string) []KeyValue {
	log.Printf("开始读取Intermediate文件:%s\n", filePath)
	inputFile, inputError := os.Open(filePath)
	if inputError != nil {
		log.Fatalf("打开文件时出错: %s", inputError.Error())
		return nil
	}
	defer inputFile.Close()
	kvs := []KeyValue{}
	inputReader := bufio.NewReader(inputFile)
	lineCount := 0
	for {
		inputString, readerError := inputReader.ReadString('\n')
		if readerError == io.EOF {
			break
		}
		// 去掉最后的换行符
		split := strings.SplitN(inputString[:len(inputString)-1], ":", 2)
		kvs = append(kvs, KeyValue{split[0], split[1]})
		lineCount += 1
	}
	log.Printf("读取完Intermediate文件，共%d行", lineCount)
	return kvs
}

func StatusUpdate(status StatusUpdateArgs) (TaskReply, bool) {
	reply := TaskReply{}
	success := call("Coordinator.WorkerStatusUpdate", &status, &reply)
	return reply, success
}

func Alive(args AliveArgs) AliveReply {
	reply := AliveReply{}
	call("Coordinator.WorkerAlive", &args, &reply)
	return reply
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
		log.Print("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
