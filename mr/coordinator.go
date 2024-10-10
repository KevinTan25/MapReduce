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
	Type             string
	numReducers      int
	PendingTask      int
	CompleteTask     int
	files            []string
	taskSent         string
	queue            chan string
	mu               sync.Mutex
	MapperTasksList  []Task
	ReducerTasksList []string
	initializeQueue  bool
	allIntFiles      []KeyValue
	McompleteArray   []string
	RcompArray       []string
	RToDo            []string
	ReducersLeft     int
	MappersLeft      int
	DoneVar          string
	RTask            string
	PendingRTask     []string
}
type Task struct {
	Done   bool
	file   string
	taskId int
	// lock sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestWork(args *Args, reply *Reply) error {
	//fmt.Println("Work requested at t=", time.Now())
	c.mu.Lock()
	// This is only ran once to initialize the queue
	if !c.initializeQueue {
		for i := 0; i < len(c.files); i++ {
			c.queue <- c.files[i]
			// Create all tasks that need to be done and set the done to be false
			t := Task{false, c.files[i], i + 1}
			c.MapperTasksList[i] = t
		}
		// Create the same tasks list for reducer
		for j := 0; j < c.numReducers; j++ {
			c.ReducerTasksList = append(c.ReducerTasksList, fmt.Sprint(j))
			c.RToDo[j] = fmt.Sprint(j)
		}
		c.initializeQueue = true
	}
	c.mu.Unlock()

	//fmt.Println("Attempting to aquire lock at t=", time.Now())
	c.mu.Lock()
	//fmt.Println("Aquired lock at t=", time.Now())

	if len(c.McompleteArray) == len(c.files) {
		c.Type = "Reducer"
	} else {
		if c.Type != "Reducer" {
			reply.ReduceTask = "Temp"
			// This code is to double check if I'm sending a file that isn't done to worker
			for {
				var temp string
				if len(c.queue) != 0 {
					temp = <-c.queue
				} else {
					break
				}
				found := false
				for _, f := range c.McompleteArray {
					if f == temp {
						found = true
						break
					}
				}
				if !found {
					reply.Input_files = temp
					break
				}
			}
		}
	}
	if c.Type == "Reducer" {
		if len(c.ReducerTasksList) != 0 {
			// Task to do passed to worker
			reply.ReduceTask = c.ReducerTasksList[0]
			change := false
			for _, i := range c.PendingRTask {
				if i == reply.ReduceTask {
					change = true
					break
				}
			}
			//fmt.Println("Pending tasks (before assignment)", c.PendingRTask)
			if change && len(c.ReducerTasksList) >= 2 {
				// reply.ReduceTask = c.ReducerTasksList[1]
				for i, _ := range c.ReducerTasksList {
					reply.ReduceTask = c.ReducerTasksList[i]
					insidePending := false
					for _, n := range c.PendingRTask {
						if reply.ReduceTask == n {
							insidePending = true
							break
						}
					}
					if !insidePending {
						break
					}
				}
			}
			c.PendingRTask = append(c.PendingRTask, reply.ReduceTask)
			//fmt.Println("Reduce Task assigned: ", reply.ReduceTask)
			//fmt.Println("Pending Tasks: ", c.PendingRTask)
		} else {
			reply.ReduceTask = "Finish"
			c.DoneVar = "Finish"
		}

		// Pass in the Intermediate Reducer
		reply.IntFiles = c.allIntFiles
	}

	reply.FuncType = c.Type
	reply.Partitions = c.numReducers
	c.taskSent = reply.Input_files
	var task *Task

	if c.Type == "Mapper" {
		// Pass in the taskId
		for j := 0; j < len(c.files); j++ {
			if reply.Input_files == c.MapperTasksList[j].file {
				reply.TaskNum = c.MapperTasksList[j].taskId
				break
			}
		}
		for _, t := range c.MapperTasksList {
			if t.file == reply.Input_files {
				task = &t
				break
			}
			if reply.Input_files == "" {
				//				task = nil
			}
		}
	}

	c.mu.Unlock()
	// start go routine that sleeps for 10 s

	go func() {
		time.Sleep(10 * time.Second)
		// under lock
		c.mu.Lock()
		defer c.mu.Unlock()

		if c.Type == "Mapper" {
			var add bool
			add = true
			if reply.Input_files == "" && len(c.McompleteArray) != len(c.files) {
				// Do nothing and wait for other mapper tasks to finish
			} else {
				for _, s := range c.McompleteArray {
					if task.file == s {
						add = false
					}
				}
				// This should run if the task isn't in the Mapper complete array
				if add && len(c.McompleteArray) != len(c.files) {
					c.queue <- reply.Input_files
				}
			}
		}
		if c.Type == "Reducer" {
			for _, i := range c.ReducerTasksList {
				c.RToDo = append(c.RToDo, i)
			}
			c.PendingRTask = c.PendingRTask[:0]
		}
	}()

	return nil
}
func (c *Coordinator) TaskFinish(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, task := range c.MapperTasksList {
		if args.FileNameKey == task.file {
			task.Done = true
			// Create an array of completeness and add it to that.
			c.McompleteArray = append(c.McompleteArray, task.file)
			c.MappersLeft--

			break
		}
	}

	// Add intermediate files to allIntFiles
	IntFile := args.IntermediateFiles
	for _, t := range IntFile {
		c.allIntFiles = append(c.allIntFiles, t)
	}

	// Temporary exit
	// if len(c.queue) == 0 {
	// 	os.Exit(3)
	// }
	//c.mu.Unlock()
	return nil
}
func (c *Coordinator) ReduceTaskFinish(args *Args, reply *Reply) error {
	c.mu.Lock()
	c.RcompArray = append(c.RcompArray, args.RedFinish)
	for i, j := range c.ReducerTasksList {
		if j == args.RedFinish {
			c.ReducerTasksList = append(c.ReducerTasksList[:i], c.ReducerTasksList[i+1:]...)
			//fmt.Println("ReducerTasksList", c.ReducerTasksList)
			break
		}
	}
	for i, j := range c.PendingRTask {
		if j == args.RedFinish {
			c.PendingRTask = append(c.PendingRTask[:i], c.PendingRTask[i+1:]...)
			break
		}
	}
	//	fmt.Println("Reduce Task ", args.RedFinish, " done")
	if len(c.RToDo) != 0 {
		c.ReducersLeft--
	}
	c.mu.Unlock()
	return nil
}

// func Wait(file string) bool {
// 	finished := true
// 	doneC := make(chan bool)
// 	go func() {
// 		time.Sleep(10 * time.Second)
// 		doneC <- false
// 	}()
// 	select {
// 		case <- doneC:
// 			finished = false
// 	}
// 	return finished
// }

// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// mr-main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	ret := false

	// Your code here.

	if c.ReducersLeft <= 0 || len(c.RToDo) <= 0 {
		if len(c.ReducerTasksList) == 0 {
			ret = true
		}
	}
	c.mu.Unlock()

	return ret
}

// create a Coordinator.
// mr-main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	//	fmt.Println("Making coordinator")
	// Your code here.
	c.numReducers = nReduce
	c.Type = "Mapper"
	c.PendingTask = len(files)
	c.CompleteTask = 0
	c.files = files
	c.taskSent = ""
	c.queue = make(chan string, len(c.files))
	c.mu = sync.Mutex{}
	c.initializeQueue = false
	c.MapperTasksList = make([]Task, len(files))
	c.RToDo = make([]string, c.numReducers)
	c.ReducersLeft = nReduce
	c.MappersLeft = len(c.files)
	c.DoneVar = ""
	c.RTask = ""

	c.server()
	return &c
}

// start a thread that listens for RPCs from worker.go
// DO NOT MODIFY
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
