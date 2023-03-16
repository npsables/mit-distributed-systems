package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

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

type Task struct {
	Task            []string
	Worker          string
	Status          string
	Reduce_map      map[int][]string
	Reduce_key      int
	Reduce_parallel int
	Time            time.Time
}

type ReduceIndex struct {
	Task       Task
	Mapkey     map[int][]string
	Reducetask string
}

const Debug = 1

func Dprintf(format string, a ...interface{}) {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
}

func Dprintln(a ...interface{}) {
	if Debug > 0 {
		fmt.Println(a...)
	}
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
