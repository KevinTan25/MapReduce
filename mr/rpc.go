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

// example to show how to declare the arguments
// and reply for an RPC.
type ExampleArgs struct {
	X int
}
type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type Args struct {
	// Mapper or Reducer
	FuncType string

	// ID Per Partition if it fails so coordinator can resend the same task
	PartitionID int

	// File name for String key
	// Values for the document should be accessed through opening the file using the file name
	FileNameKey string

	// Key for for the reduce function. Should be from []KeyValue struct
	ReduceKey string

	// Value for reduce function. Should be from []KeyValue struct.
	Iterator string

	// List of all intermediate files
	IntermediateFiles []KeyValue

	// Reducer Task that finished
	RedFinish string
}
type Reply struct {
	// Mapper or Reducer
	FuncType string

	// Return for the mapper
	ReturnKey []KeyValue

	// Return for the reducer
	ReduceStr string

	// Input files
	// Files []string
	Input_files string

	// Number of partitions or number of reducers
	Partitions int

	// Update if Task is done
	// TaskDone chan bool

	// TaskID to create intermediate files
	TaskNum int

	// KeysList to store what keys the reducer needs to do work
	// ReducerKeyList []int

	// Intermediate Files for Reducer in Worker
	IntFiles []KeyValue

	// The task id that reduce needs to do
	ReduceTask string
}
// type Finish struct {
// 	nReducer int

// 	// Stores if it is complete
// 	complete int
// }

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
