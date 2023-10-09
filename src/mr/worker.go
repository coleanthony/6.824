package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	// uncomment to send the Example RPC to the master.
	// CallExample()
	for {
		args := AskTaskArgs{}
		args.X = 1
		reply := AskTaskReply{}
		call("Master.AskTaskhandler", &args, &reply)
		switch reply.Replytasktype {
		case Tasktype_Map:
			handleMapTask(mapf, reply.Reducenum, reply.MapFilename, reply.Replytaskindex)
		case Tasktype_Reduce:
			handleReduceTask(reducef, reply.Replytaskindex, reply.ReduceFilenames)
		case Tasktype_Wait:
			time.Sleep(time.Millisecond * 10)
		case Tasktype_End:
			//handle task_type end
			log.Fatal("no task to get")
		}
	}
}

/*
func CallAskTask() (AskTaskArgs, AskTaskReply) {
	//worker向master通过rpc机制申请一个任务
	args := AskTaskArgs{}
	args.X = 1
	reply := AskTaskReply{}
	call("Master.AskTaskhandler", &args, &reply)
	return args, reply
}*/

func handleMapTask(mapf func(string, string) []KeyValue, reducenum int, MapFilename string, taskindex int) {
	//handle the map task,and report to master
	//fmt.Println("workder start to handle map task:", taskindex)
	intermediate := []KeyValue{}
	file, err := os.Open(MapFilename)
	if err != nil {
		log.Fatalf("cannot open %v", MapFilename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", MapFilename)
	}
	file.Close()

	//do map task
	kva := mapf(MapFilename, string(content))
	intermediate = append(intermediate, kva...)

	filenames := make([]string, reducenum)
	intermediatename := "mr_" + strconv.Itoa(taskindex) + "_"
	files := make([]*os.File, reducenum)

	for i := 0; i < reducenum; i++ {
		filenames[i] = intermediatename + strconv.Itoa(i)
		files[i], _ = os.Create(filenames[i])
	}

	//write intermediate data to files
	for _, kv := range intermediate {
		tofile := ihash(kv.Key) % reducenum
		enc := json.NewEncoder(files[tofile])
		enc.Encode(&kv)
	}

	//report map task finished
	CallfinishTask(filenames, Tasktype_Map, taskindex)
}

func handleReduceTask(reducef func(string, []string) string, taskindex int, reduceFilenames []string) {
	//handle the reduce task,and report to master
	//fmt.Println("workder start to handle reduce task", taskindex)
	//fmt.Println(reduceFilenames)
	reducefiles := reduceFilenames
	intermediate := []KeyValue{}
	files := make([]*os.File, len(reducefiles))

	for i := 0; i < len(reducefiles); i++ {
		files[i], _ = os.Open(reducefiles[i])
		dec := json.NewDecoder(files[i])
		kv := KeyValue{}
		for {
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(taskindex)
	ofile, _ := os.Create(oname)

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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
	tempfilenames := []string{}
	tempfilenames = append(tempfilenames, oname)

	//report reduce task finished
	CallfinishTask(tempfilenames, Tasktype_Reduce, taskindex)
}

func CallfinishTask(intermediatefilenames []string, finishtasktype Tasktype, taskindex int) error {
	args := FinishTaskArgs{
		Reportfilenames: intermediatefilenames,
		Finishtasktype:  finishtasktype,
		Taskindex:       taskindex,
	}
	reply := FinishTaskReply{}
	reply.X = 1
	call("Master.FinishTaskhandler", &args, &reply)
	return nil
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
