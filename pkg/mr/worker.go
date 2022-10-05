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
			fmt.Println("Got idle! Do idle")
			time.Sleep(2 * time.Second)
		default:
			fmt.Println("Do idle")
			time.Sleep(1 * time.Second)
		}
	}
}

func doMapping(mapf func(string, string) []KeyValue, task Task) {
	fmt.Println("Do Mapping!!!", task.Task)
	for {
		select {
		case <-time.After(100*time.Second - time.Since(task.Time)):
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

			// fmt.Printf("EXAMPLE MAP: %v \n", intermediate[0])

			data, _ := json.MarshalIndent(intermediate, "", "  ")
			reduce_task := fmt.Sprintf("reduce_%v.txt", rand.Int())
			_ = ioutil.WriteFile(reduce_task, data, 0644)

			CallPushReduce(mapkey, reduce_task)
			CallDone(task)
			return
		}
	}
}

func doReducing(reducef func(string, []string) string, task Task) {
	fmt.Println("Do Reducing!!!")
	for {
		select {
		case <-time.After(1*time.Second - time.Since(task.Time)):
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
					panic(err)
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
			fmt.Println("Done Reduce")
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

func CallPushReduce(mapkey map[int][]string, reducetask string) {
	fmt.Println("Calling Push Reduce")
	rq := ReduceIndex{
		Mapkey:     mapkey,
		Reducetask: reducetask,
	}
	var rply bool
	call("Coordinator.PushReduce", &rq, &rply)
}

func CallDone(task Task) {
	fmt.Println("Calling done")
	var reply bool
	call("Coordinator.DoneTask", &task, &reply)
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
