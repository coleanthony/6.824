package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type AskTaskArgs struct {
	//what args do you need
	X int
}

type AskTaskReply struct {
	//what task info to reply
	Reducenum       int
	Replytasktype   Tasktype
	Replytaskindex  int
	MapFilename     string
	ReduceFilenames []string
}

type FinishTaskArgs struct {
	Reportfilenames []string
	Finishtasktype  Tasktype
	Taskindex       int
}

type FinishTaskReply struct {
	X int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
