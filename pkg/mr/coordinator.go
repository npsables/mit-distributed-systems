package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu            sync.Mutex
	mapFlag       bool
	timeout_tasks chan Task

	// task name and worker
	tasks_state map[string]string
	job_state   chan interface{}
	done        chan bool
	// files and workers
	// maps have to return file name for reduce, since ihash only available in workers
	map_tasks []string

	// `files written by map` and workers
	reduce_tasks    []string
	reduce_parallel int
	map_keys        map[int][]string
}

const (
	WAIT    = "wait"
	DONE    = "done"
	RUNNING = "running"
	FAIL    = "failed"
)

// Your code here -- RPC handlers for the worker to call.

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
	// Your code here.
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.reduce_parallel = nReduce
	c.createTaskList(files)
	c.server()
	return &c
}

func removeFirst(lists []string) []string {
	lists = append(lists[:0], lists[1:]...)
	fmt.Println("Number of tasks left: ", len(lists))
	return lists
}

func (c *Coordinator) createTaskList(l []string) {
	fmt.Printf("Number of tasks: %v \n", len(l))
	c.mapFlag = false
	c.job_state = make(chan interface{})
	c.timeout_tasks = make(chan Task)
	c.tasks_state = make(map[string]string)
	c.done = make(chan bool)
	c.map_tasks = l
	c.map_keys = make(map[int][]string)
	for i := 0; i < c.reduce_parallel; i++ {
		c.map_keys[i] = []string{}
	}
	c.reduce_tasks = make([]string, 0)
	go c.taskManager()
}

func (c *Coordinator) taskManager() {
	// this need to control tasks flow
	for {
		select {
		// call job state imidiately whenever a task is done!
		case <-c.job_state:
			c.mu.Lock()
			if len(c.map_tasks) == 0 && len(c.map_keys) == 0 && len(c.tasks_state) == 0 {
				// close program
				fmt.Println("Close program now!")
				c.done <- true
			} else if len(c.map_tasks) == 0 && len(c.map_keys) > 0 && len(c.tasks_state) == 0 {
				// change job to run reduce
				if !c.mapFlag {
					for _, v := range c.map_keys {
						fmt.Println("Num of unique keys: ", len(v))
					}
					fmt.Println("Done Mapping, change to Transfer Reduce!")
					fmt.Println("Full reduce tasks: ", c.reduce_tasks)
					c.mapFlag = true
				}
			}
			c.mu.Unlock()
			// this to terminate timeout tasks
		case task := <-c.timeout_tasks:
			if task.Status == "reducing" {
				c.mu.Lock()
				c.reduce_tasks = append(c.reduce_tasks, task.Task...)
				// NOTE: this here enough of unique key!
				// for k, v := range task.Reduce_map {
				// 	c.map_keys[k] = v
				// }
				c.mu.Unlock()
			} else if task.Status == "mapping" {
				c.mu.Lock()
				c.map_tasks = append(c.map_tasks, task.Task...)
				c.mu.Unlock()
			}
		}
	}
}

func (c *Coordinator) PushReduce(reduce_keymap *ReduceIndex, _ *bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.reduce_tasks) == 0 {
		c.reduce_tasks = append(c.reduce_tasks, reduce_keymap.Reducetask)
	} else {
		m := make_map(c.reduce_tasks, []string{reduce_keymap.Reducetask})
		var tmpl []string
		for key := range m {
			tmpl = append(tmpl, key)
		}
		c.reduce_tasks = tmpl
	}

	// fmt.Printf("%v", c.reduce_tasks)
	for i := 0; i < c.reduce_parallel; i++ {
		var tmpl []string
		m := make_map(c.map_keys[i], reduce_keymap.Mapkey[i])
		for key := range m {
			tmpl = append(tmpl, key)
		}
		c.map_keys[i] = tmpl
	}

	return nil
}

func make_map(lst ...[]string) map[string]bool {
	rtnmap := make(map[string]bool)
	for _, l := range lst {
		for _, key := range l {
			// if _, ok := rtnmap[key]; !ok {
			rtnmap[key] = true
			// }
		}
	}
	return rtnmap
}

func (c *Coordinator) DoneTask(task *Task, _ *bool) error {
	// delete the map of task name
	var ok bool

	// what if not the same worker? noway right?
	switch task.Status {
	case "mapping":
		c.mu.Lock()
		defer c.mu.Unlock()
		if _, ok = c.tasks_state[task.Task[0]]; !ok {
			panic(ok)
		}
		delete(c.tasks_state, task.Task[0])
		c.job_state <- true
		return nil

	case "reducing":
		c.mu.Lock()
		defer c.mu.Unlock()
		key := fmt.Sprintf("%v", task.Reduce_key)
		if _, ok = c.tasks_state[key]; !ok {
			panic(ok)
		}
		delete(c.tasks_state, key)
		c.job_state <- true
		return nil
	}

	return nil
}

func (c *Coordinator) GetTask(worker *string, reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mapFlag && len(c.reduce_tasks) != 0 {
		task_name := c.reduce_tasks
		var k int

		var v []string
		if len(c.map_keys) == 0 {
			*reply = Task{
				Worker: "",
				Status: "idle",
			}
			return nil
		}
		for key, value := range c.map_keys {
			k = key
			v = value
			break
		}

		fmt.Println("Take reduce task pls", k)
		worker_task := Task{
			Task:            task_name,
			Worker:          *worker,
			Status:          "reducing",
			Reduce_key:      k,
			Reduce_map:      map[int][]string{k: v},
			Reduce_parallel: c.reduce_parallel,
			// worker also has to terminate, this is for it
			Time: time.Now(),
		}

		delete(c.map_keys, k)
		state_key := fmt.Sprintf("%v", k)
		c.tasks_state[state_key] = *worker
		go c.taskTracker(&worker_task)
		// c.reduce_tasks = removeFirst(c.reduce_tasks)
		*reply = worker_task
	} else if c.mapFlag && len(c.map_keys) == 0 {
		fmt.Println("Go terminate!")
		*reply = Task{
			Worker: "",
			Status: "terminate",
		}
	} else if len(c.map_tasks) == 0 && len(c.map_keys) != 0 {
		fmt.Println("The flag is ", c.mapFlag, "Go idle")
		*reply = Task{
			Worker: "",
			Status: "idle",
		}
	} else {
		fmt.Println("Take map task pls")
		task_name := c.map_tasks[0]
		worker_task := Task{
			Task:            []string{task_name},
			Worker:          *worker,
			Status:          "mapping",
			Reduce_parallel: c.reduce_parallel,
			Time:            time.Now(),
		}
		c.tasks_state[task_name] = *worker
		go c.taskTracker(&worker_task)
		c.map_tasks = removeFirst(c.map_tasks)
		*reply = worker_task
	}
	return nil
}

func (c *Coordinator) taskTracker(t *Task) {
	<-time.After(100 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
	var taskname string

	if t.Status == "mapping" {
		taskname = t.Task[0]
	} else {
		taskname = fmt.Sprintf("%v", t.Reduce_key)
	}

	if _, ok := c.tasks_state[taskname]; ok {
		// if val == t.worker {
		fmt.Println("Task timeout ", t.Task)
		c.timeout_tasks <- *t
		// }
	}
}
