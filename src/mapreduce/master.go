package mapreduce

import (
	"container/list"
	"log"
	"os"
)

var Logger *log.Logger

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			DPrintf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func listenNewWorker(mr *MapReduce, worker chan string) {
	for {
		aviliable := <-mr.registerChannel
		worker <- aviliable
	}
}

func doMap(mr *MapReduce, worker chan string) {
	//this chan is to ensure that all map were done
	done := make(chan int)
	for i := 0; i < mr.nMap; i++ {
		//begin number of nMap call simultaneously
		//start nMap goroutine at the same time
		go func(i int) {
			//if not aviliable worker, loooooop
			for avil := range worker {
				args := DoJobArgs_new(mr.file, Map, i, mr.nReduce)
				var reply DoJobReply
				ok := call(avil, "Worker.DoJob", args, &reply)
				if ok {
					done <- 1
					worker <- avil //now the avil is free, so other thread can use it.
					/*
					 * Since call() may return false, must use for range chan to get worker.
					 * If call always return true, the code can rewirte as follow:
					 *
					 * `avil:= <worker` and don't need a break
					 */
					break // break the for range
				} else {
					DPrintf("No aviliable worker for Map %d\n", args.JobNumber)
				}
			}
		}(i)
	}
	//wait for all call return ok
	for i := 0; i < mr.nMap; i++ {
		<-done
	}
}

func doReduce(mr *MapReduce, worker chan string) {
	done := make(chan int)
	for i := 0; i < mr.nReduce; i++ {
		go func(i int) {
			for avil := range worker {
				args := DoJobArgs_new(mr.file, Reduce, i, mr.nMap)
				var reply DoJobReply
				ok := call(avil, "Worker.DoJob", args, &reply)
				if ok {
					done <- 1
					worker <- avil
					break
				} else {
					DPrintf("No useful worker for Reduce %d\n", args.JobNumber)
				}
			}
		}(i)
	}

	for i := 0; i < mr.nReduce; i++ {
		<-done
	}
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here

	worker := make(chan string)

	//recive new registerWorker
	go listenNewWorker(mr, worker)

	//create nMap goroutines to do DoJob()
	doMap(mr, worker)

	doReduce(mr, worker)

	return mr.KillWorkers()
}

func InitLog() {
	file, err := os.Create("log.txt")
	if err != nil {
		log.Fatalf("%s\n", "log file error")
	}
	Logger = log.New(file, "", 0)
}
