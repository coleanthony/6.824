/*
1、master节点将任务需要执行map任务的文件分配给多个worker节点，然后等待任务完成。
2、多个worker节点完成map任务后，按照要求生成中间文件，并将其信息告知master节点。
3、master节点在所有的map任务完成后，将中间文件作为reduce任务的输入分发给多个worker节点。
4、所有worker节点完成任务后告知master节点，master完成mapreduce任务。

注意：锁、timeout判断
*/

package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Tasktype int
type State int

//节点状态
//目前可不定义
const (
	Working = iota
	Timeout
)

//任务状态
const (
	NotStartYet = iota
	Executing
	Finished
)

//任务类型
var (
	Tasktype_Map    Tasktype = 0
	Tasktype_Reduce Tasktype = 1
	Tasktype_Wait   Tasktype = 2
	Tasktype_End    Tasktype = 3
)

type Master struct {
	// Your definitions here.
	Mapnum                int              //num of input files for map task
	Reducenum             int              //num of workers for reduce task
	mapfinished           int              //map task already finished
	reducefinished        int              //reduce task already finished
	curtaskId             int              //the present taskid
	Mux                   sync.Mutex       //mutex
	MapStateRecord        map[int]*Task    //record the state of map task
	ReduceStateRecord     map[int]*Task    //record the state of reduce task
	intermediateReduceMsg map[int][]string //record intemediate massage of map result
	//TaskPool              []*Task          //task pool
	//TaskRecord            map[int]*Task    //record the task,if the task is finished,the record will be removed
	Alldone bool //all tasks are done
}

type Task struct {
	//definitions of task
	state           State    //task state
	tasktype        Tasktype //task type
	taskindex       int      //a unique index of a task
	MapFilename     string   //mapfilename
	reduceFilenames []string //reduce file names
	Reducename      int      //reduce index
}

/*
func (m *Master) GenerateTaskId() int {
	m.Mux.Lock()
	defer m.Mux.Unlock()
	rt := m.curtaskId
	m.curtaskId++
	return rt
}

func (m *Master) TaskEmpty() bool {
	m.Mux.Lock()
	defer m.Mux.Unlock()
	return len(m.TaskPool) == 0
}

func (m *Master) popTask() *Task {
	m.Mux.Lock()
	defer m.Mux.Unlock()
	if m.TaskEmpty() {
		return nil
	}
	res := m.TaskPool[0]
	res.state = Executing
	m.TaskPool = m.TaskPool[1:]
	return res
}

func (m *Master) pushTask(task *Task) error {
	m.Mux.Lock()
	defer m.Mux.Unlock()
	m.TaskPool = append(m.TaskPool, task)
	return nil
}*/

// Your code here -- RPC handlers for the worker to call. change the exampleargs and examplereply
func (m *Master) AskTaskhandler(args *AskTaskArgs, reply *AskTaskReply) error {
	//处理worker callasktask的请求
	m.Mux.Lock()
	defer m.Mux.Unlock()

	if m.mapfinished == m.Mapnum {
		//发送reduce任务
		reply.Replytasktype = Tasktype_Reduce
		//k:id  v:task
		for k, v := range m.ReduceStateRecord {
			if m.ReduceStateRecord[k].state == NotStartYet {
				//fmt.Println("master:distribute reduce tasks:", v.taskindex)
				//fmt.Println(v.reduceFilenames)
				m.ReduceStateRecord[k].state = Executing
				reply.Replytaskindex = v.taskindex
				reply.MapFilename = v.MapFilename
				reply.ReduceFilenames = v.reduceFilenames
				//fmt.Println(reply.Replytaskindex, reply.reduceFilenames)
				reply.Reducenum = m.Reducenum
				go m.TaskTimeoutHandler(v.tasktype, v.taskindex)
				return nil
			}
		}
		reply.Replytasktype = Tasktype_Wait
		return nil
	} else {
		//发送map任务
		reply.Replytasktype = Tasktype_Map
		for k, v := range m.MapStateRecord {
			if m.MapStateRecord[k].state == NotStartYet {
				//fmt.Println("master:distribute map tasks:", v.taskindex)
				m.MapStateRecord[k].state = Executing
				reply.Replytaskindex = v.taskindex
				reply.MapFilename = v.MapFilename
				reply.ReduceFilenames = v.reduceFilenames
				reply.Reducenum = m.Reducenum
				go m.TaskTimeoutHandler(v.tasktype, v.taskindex)
				return nil
			}
		}
		reply.Replytasktype = Tasktype_Wait
		return nil
	}
	return nil
}

func (m *Master) FinishTaskhandler(args *FinishTaskArgs, reply *FinishTaskReply) error {
	m.Mux.Lock()
	defer m.Mux.Unlock()

	if args.Finishtasktype == Tasktype_Map {
		//处理map任务完成返回
		//将map中间结果存在intermediateReduceMsg
		if m.MapStateRecord[args.Taskindex].state == Executing {
			//fmt.Println("finish map task", args.Taskindex)
			m.MapStateRecord[args.Taskindex].state = Finished
			m.mapfinished++
			//fmt.Println(args.Reportfilenames)
			for _, filename := range args.Reportfilenames {
				idx := strings.LastIndex(filename, "_")
				taskid, err := strconv.Atoi(filename[idx+1:])
				if err != nil {
					log.Fatal(err)
				}
				m.intermediateReduceMsg[taskid] = append(m.intermediateReduceMsg[taskid], filename)
			}

			//当所有map任务完成，产生reduce任务
			if m.mapfinished == m.Mapnum {
				for i := 0; i < m.Reducenum; i++ {
					//fmt.Println(m.intermediateReduceMsg[i])
					t := Task{
						state:           NotStartYet,
						tasktype:        Tasktype_Reduce,
						taskindex:       i,
						MapFilename:     "",
						reduceFilenames: m.intermediateReduceMsg[i],
						Reducename:      i,
					}
					m.ReduceStateRecord[i] = &t
					//fmt.Println(m.ReduceStateRecord[i].reduceFilenames)
				}
			}
		}
	} else {
		//处理reduce任务完成返回
		//fmt.Println("finish reduce task", args.Taskindex)
		if m.ReduceStateRecord[args.Taskindex].state == Executing {
			m.ReduceStateRecord[args.Taskindex].state = Finished
			m.reducefinished++
			if m.reducefinished == m.Reducenum {
				m.Alldone = true
			}
		}
	}
	return nil
}

func (m *Master) TaskTimeoutHandler(tasktype Tasktype, taskindex int) error {
	//handle tasktime out
	time.Sleep(time.Second * 10)
	m.Mux.Lock()
	defer m.Mux.Unlock()

	if tasktype == Tasktype_Map {
		if m.MapStateRecord[taskindex].state != Finished {
			m.MapStateRecord[taskindex].state = NotStartYet
		}
	} else if tasktype == Tasktype_Reduce {
		if m.ReduceStateRecord[taskindex].state != Finished {
			m.ReduceStateRecord[taskindex].state = NotStartYet
		}
	}
	return nil
}

/*
func (m *Master) GenerateMapTasks(files []string) {
	m.Mux.Lock()
	defer m.Mux.Unlock()

	for _, f := range files {
		id := m.GenerateTaskId()
		m.TaskStateRecord[id] = NotStartYet
		t := Task{
			state:           NotStartYet,
			tasktype:        Tasktype_Map,
			taskindex:       id,
			MapFilename:     f,
			reduceFilenames: []string{},
			Reducename:      -1,
		}
		m.pushTask(&t)
		m.TaskRecord[id] = &t
	}
	fmt.Println("generate maptasks successfully!")
}

func (m *Master) GenerateReduceTasks() {
	m.Mux.Lock()
	defer m.Mux.Unlock()
	//generate reduce tasks and put them into taskpools
	for i := 0; i < m.Reducenum; i++ {
		id := m.GenerateTaskId()
		m.TaskStateRecord[id] = NotStartYet
		t := Task{
			state:           NotStartYet,
			tasktype:        Tasktype_Reduce,
			taskindex:       id,
			MapFilename:     "",
			reduceFilenames: m.intermediateReduceMsg[i],
			Reducename:      i,
		}
		m.pushTask(&t)
		m.TaskRecord[id] = &t
	}
	fmt.Println("generate reducetasks successfully!")
}
*/

// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	//log.Println("master:listen successfully")
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	//Your code here.  expected to return true
	//returns true when the MapReduce job is completely finished
	m.Mux.Lock()
	defer m.Mux.Unlock()
	if m.Alldone {
		//when all the reduce tasks are finished,the mapreduce work is done
		ret = true
	}
	time.Sleep(200 * time.Millisecond)
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		Mapnum:            len(files),
		Reducenum:         nReduce,
		mapfinished:       0,
		reducefinished:    0,
		Mux:               sync.Mutex{},
		MapStateRecord:    make(map[int]*Task),
		ReduceStateRecord: make(map[int]*Task),
		//TaskPool:              []*Task{},
		//TaskRecord:            make(map[int]*Task),
		intermediateReduceMsg: make(map[int][]string),
		Alldone:               false,
		curtaskId:             0,
	}
	/*
		fmt.Println("master:initialize the state of tasks")
		fmt.Println("file num:", len(files))
		fmt.Println("reduce num:", nReduce)
	*/

	for i := 0; i < len(files); i++ {
		t := Task{
			state:           NotStartYet,
			tasktype:        Tasktype_Map,
			taskindex:       i,
			MapFilename:     files[i],
			reduceFilenames: []string{},
			Reducename:      -1,
		}
		m.MapStateRecord[i] = &t
		//fmt.Println(m.MapStateRecord[i].MapFilename)
	}

	/*
		for _, v := range m.MapStateRecord {
			fmt.Println(v.MapFilename, v.taskindex)
		}*/

	m.server()
	return &m
}
