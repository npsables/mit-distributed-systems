package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"strconv"
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
	// CallExample()
	rand.Seed(time.Now().UnixNano())
	id := strconv.Itoa(rand.Int())
	for {
		switch task := CallGetTask(id); task.Status {
		case "mapping":
			doMapping(mapf, task)
			time.Sleep(time.Second)
		case "reducing":
			doReducing(reducef, task)
			time.Sleep(time.Second)
		case "terminate":
			os.Exit(1)
		case "idle":
			Dprintln("Got idle! Do idle")
			time.Sleep(3 * time.Second)
		default:
			Dprintln("Do idle")
			time.Sleep(1 * time.Second)
		}
	}
}

func doMapping(mapf func(string, string) []KeyValue, task Task) {
	Dprintln("Do Mapping!!!", task.Task)
	timeout := time.After(20 * time.Second)
	for {
		select {
		case <-timeout:
			return
		default:
			intermediate := []KeyValue{}
			mapkey := make(map[int][]string)

			for _, filename := range task.Task {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				kva := mapf(filename, string(content))
				for _, value := range kva {
					mapkey[ihash(value.Key)%task.Reduce_parallel] = append(mapkey[ihash(value.Key)%task.Reduce_parallel], value.Key)
				}
				intermediate = append(intermediate, kva...)
			}

			// Dprintf("EXAMPLE MAP: %v \n", intermediate[0])

			data, _ := json.MarshalIndent(intermediate, "", "  ")
			reduce_task := fmt.Sprintf("reduce_%v", strings.Split(task.Task[0], "/")[len(strings.Split(task.Task[0], "/"))-1])
			_ = ioutil.WriteFile(reduce_task, data, 0644)

			CallPushReduce(mapkey, reduce_task, task)
			// CallDone(task)
			return
		}
	}
}

func doReducing(reducef func(string, []string) string, task Task) {
	Dprintln("Do Reducing!!!")
	timeout := time.After(20 * time.Second)
	for {
		select {
		case <-timeout:
			// TODO: It is not timeout what the hell?
			return
		default:
			// TODO: something so wrong here
			intermediate := []KeyValue{}
			for _, file := range task.Task {
				intermediatefile := []KeyValue{}
				var jsonFile *os.File
				var err error

				if jsonFile, err = os.Open(file); err != nil {
					fmt.Printf("ERROR %v", err)
				}
				defer jsonFile.Close()

				byteValue, _ := ioutil.ReadAll(jsonFile)
				json.Unmarshal(byteValue, &intermediatefile)

				// select only key in task.Reduce_key
				for _, filekv := range intermediatefile {
					for _, key := range task.Reduce_map[task.Reduce_key] {
						if filekv.Key == key {
							intermediate = append(intermediate, filekv)
						}
					}
				}
			}

			oname := fmt.Sprintf("mr-out-%v", task.Reduce_key)
			ofile, _ := os.Create(oname)

			sort.Sort(ByKey(intermediate))
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
			Dprintln("Done Reduce")
			CallDone(task)
			return
		}
	}
}

func CallGetTask(id string) Task {
	reply := Task{}
	call("Coordinator.GetTask", &id, &reply)
	return reply
}

func CallPushReduce(mapkey map[int][]string, reducetask string, task Task) {
	Dprintln("Calling Push Reduce", task.Task)
	rq := ReduceIndex{
		Task:       task,
		Mapkey:     mapkey,
		Reducetask: reducetask,
	}
	var rply bool
	call("Coordinator.PushReduce", &rq, &rply)
}

func CallDone(task Task) {
	Dprintln("Calling Done Reduce")
	var reply bool
	call("Coordinator.DoneReduceTask", &task, &reply)
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
	Dprintf("reply.Y %v\n", reply.Y)
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
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	Dprintln(err)
	return false
}
