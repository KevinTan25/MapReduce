package mr

import (
	//"bufio"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"

	//"strings"
	"encoding/json"
	"math/rand"
	"time"

	//"sync"
	"io/ioutil"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// mr-main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	id := rand.Int()

	// Your worker implementation here.
	// args := Args{}
	// reply := Reply{}
	// fin := Finish{}

	// To keep requesting work once it is done
	for {
		// Create a seperate function to return reply
		var reply *Reply
		if reply = Request(id); reply == nil {
			log.Print("Cannot contact coordinator")
			return
		}
		if reply.ReduceTask == "Finish" {
			//fmt.Println("Broke the Worker")
			break
		}
		timeKeep := time.Now()

		if reply.FuncType == "Mapper" {
			// Create intermediate files to write to
			for i := 0; i < reply.Partitions; i++ {
				_, err := os.Create(fmt.Sprint("mr-", reply.TaskNum, "-", i))
				if err != nil {
					continue
				}
			}
			intermediate := []KeyValue{}
			args := Args{}
			input := reply.Input_files
			readFile, err := os.Open(input)
			if err != nil {
				return
			}
			//mu.Lock()
			content, err := ioutil.ReadAll(readFile)
			if err != nil {
				//log.Fatalf("cannot read %v", input)
				//log.Println("cannot read %v", input)
			}
			readFile.Close()
			kva := mapf(input, string(content))
			intermediate = append(intermediate, kva...)

			for _, v := range intermediate {
				// Need to hash the values and create the pairs
				partition_id := ihash(v.Key) % reply.Partitions
				pairs := KeyValue{v.Key, v.Value}

				path := fmt.Sprint("mr-", reply.TaskNum, "-", partition_id)
				f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0777)
				enc := json.NewEncoder(f)
				enc.Encode(&pairs)
			}

			// Add the files to args so coordinator can get the intermediate files
			for i := 0; i < reply.Partitions; i++ {
				// Key = The partition id so I know what the partition the file should go into
				// Value = The actual file name
				k := KeyValue{fmt.Sprint(i), fmt.Sprint("mr-", reply.TaskNum, "-", i)}
				args.IntermediateFiles = append(args.IntermediateFiles, k)
			}

			readFile.Close()
			// Call an RPC call for Finished task in coordinator
			// If the worker takes longer than 10 seconds, it won't mark as done
			if !time.Now().After(timeKeep.Add(time.Second * 10)) {
				args.FileNameKey = reply.Input_files
				call("Coordinator.TaskFinish", &args, &reply)
			}
			if time.Now().After(timeKeep.Add(time.Second * 10)) {
				for j := 0; j < reply.Partitions; j++ {
					os.Remove(fmt.Sprint("mr-", reply.TaskNum, "-", j))
				}
			}
		}

		if reply.FuncType == "Reducer" {

			reducerTaskID := reply.ReduceTask
			var fileToOpen []string
			intF := reply.IntFiles
			for i, k := range intF {
				if intF[i].Key == reducerTaskID {
					fileToOpen = append(fileToOpen, k.Value)
				}
			}
			var intermediate []KeyValue
			for _, fi := range fileToOpen {
				newFile, _ := os.Open(fi)

				dec := json.NewDecoder(newFile)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				newFile.Close()
			}
			sort.Sort(ByKey(intermediate))

			ofile, _ := os.Create(fmt.Sprint("mr-out-", reducerTaskID))
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

				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
				defer ofile.Close()
			}
			args := Args{}
			args.RedFinish = reply.ReduceTask
			/*if !time.Now().After(timeKeep.Add(time.Second * 10)) {
				call("Coordinator.ReduceTaskFinish", &args, &reply)
			} else {
				//fmt.Println("Reducer didn't finish: ", reducerTaskID)
				os.Remove(fmt.Sprint("mr-out-", reducerTaskID))
			}*/
			call("Coordinator.ReduceTaskFinish", &args, &reply)
		}

		// uncomment to send the Example RPC to the coordinator.
		// CallExample()
	}
}

func Request(id int) *Reply {
	//fmt.Println("worker", id, "requesting work")
	functioncall := "Coordinator.RequestWork"
	args := Args{}
	reply := Reply{}
	if response := call(functioncall, &args, &reply); !response {
		return nil
	}

	return &reply
}


// example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
// DO NOT MODIFY
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

	fmt.Println("Unable to Call", rpcname, "- Got error:", err)
	return false
}
