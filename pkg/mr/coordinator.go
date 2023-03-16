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
	Dprintln("Number of tasks left: ", len(lists))
	return lists
}

func (c *Coordinator) createTaskList(l []string) {
	Dprintf("Number of tasks: %v \n", len(l))
	c.mapFlag = false
	c.job_state = make(chan interface{}, 3)
	c.timeout_tasks = make(chan Task, 3)
	c.tasks_state = make(map[string]string)
	c.done = make(chan bool, 3)
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
		case <-c.job_state:
			c.mu.Lock()
			if len(c.map_tasks) == 0 && len(c.map_keys) == 0 && len(c.tasks_state) == 0 {
				// close program
				Dprintln("Close program now!")
				c.done <- true
			} else if len(c.map_tasks) == 0 && len(c.map_keys) > 0 && len(c.tasks_state) == 0 {
				// change job to run reduce
				if !c.mapFlag {
					for _, v := range c.map_keys {
						Dprintln("Num of unique keys: ", len(v))
					}

					/// Crash test not done here wtf?????
					Dprintln("Done mapping! Full reduce tasks: ", len(c.reduce_tasks), c.reduce_tasks)
					c.mapFlag = true
				}
			}
			c.mu.Unlock()

		case task := <-c.timeout_tasks:
			if task.Status == "reducing" {
				c.mu.Lock()
				c.map_keys[task.Reduce_key] = task.Reduce_map[task.Reduce_key]
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
	// if len(c.reduce_tasks) == 0 {
	c.reduce_tasks = append(c.reduce_tasks, reduce_keymap.Reducetask)
	// } else {
	// 	m := make_map(c.reduce_tasks, []string{reduce_keymap.Reducetask})
	// 	// var tmpl []string
	// 	// for key := range m {
	// 	// 	tmpl = append(tmpl, key)
	// 	// }
	// 	c.reduce_tasks = tmpl
	// }

	// Dprintf("%v", c.reduce_tasks)
	for i := 0; i < c.reduce_parallel; i++ {
		var tmpl []string
		m := make_map(c.map_keys[i], reduce_keymap.Mapkey[i])
		for key := range m {
			tmpl = append(tmpl, key)
		}
		c.map_keys[i] = tmpl
	}

	task := reduce_keymap.Task

	// Done mapping here
	if _, ok := c.tasks_state[task.Task[0]]; !ok {
		fmt.Println("SOMETHING REALLY WRONG")
	}
	delete(c.tasks_state, task.Task[0])
	c.mu.Unlock()
	c.job_state <- true

	return nil
}

func make_map(lst ...[]string) map[string]bool {
	rtnmap := make(map[string]bool)
	for _, l := range lst {
		for _, key := range l {
			rtnmap[key] = true
		}
	}
	return rtnmap
}

func (c *Coordinator) DoneReduceTask(task *Task, _ *bool) error {
	// delete the map of task name
	var ok bool

	// what if not the same worker? noway right?
	key := fmt.Sprintf("%v", task.Reduce_key)

	c.mu.Lock()
	if _, ok = c.tasks_state[key]; !ok {
		panic(ok)
	}
	delete(c.tasks_state, key)
	c.mu.Unlock()
	c.job_state <- true
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

		Dprintln("Take reduce task pls", k)
		worker_task := Task{
			Task:            task_name,
			Worker:          *worker,
			Status:          "reducing",
			Reduce_key:      k,
			Reduce_map:      map[int][]string{k: v},
			Reduce_parallel: c.reduce_parallel,
			// worker also has to terminate, this is for it
			// Time: time.Now(),
		}

		delete(c.map_keys, k)
		state_key := fmt.Sprintf("%v", k)
		c.tasks_state[state_key] = *worker
		go c.taskTracker(&worker_task)
		*reply = worker_task
	} else if c.mapFlag && len(c.map_keys) == 0 {
		// this actually never happpends
		Dprintln("Go terminate!")
		*reply = Task{
			Worker: "",
			Status: "terminate",
		}
	} else if !c.mapFlag && len(c.map_tasks) == 0 && len(c.map_keys) != 0 {
		Dprintln("The flag is ", c.mapFlag, "Go idle")
		*reply = Task{
			Worker: "",
			Status: "idle",
		}
	} else {
		Dprintln("Take map task pls")
		task_name := c.map_tasks[0]
		worker_task := Task{
			Task:            []string{task_name},
			Worker:          *worker,
			Status:          "mapping",
			Reduce_parallel: c.reduce_parallel,
			// Time:            time.Now(),
		}
		c.tasks_state[task_name] = *worker
		go c.taskTracker(&worker_task)
		c.map_tasks = removeFirst(c.map_tasks)
		*reply = worker_task
	}
	return nil
}

func (c *Coordinator) taskTracker(t *Task) {
	<-time.After(20 * time.Second)
	var taskname string

	if t.Status == "mapping" {
		taskname = t.Task[0]
	} else {
		taskname = fmt.Sprintf("%v", t.Reduce_key)
	}
	c.mu.Lock()
	if _, ok := c.tasks_state[taskname]; ok {
		c.mu.Unlock()
		Dprintln("Task timeout ", t.Task)
		c.timeout_tasks <- *t
		return
	}
	c.mu.Unlock()
}
