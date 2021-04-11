package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	NotStarted = iota
	Started
	Finished
)

type MapTask struct {
	filename string
	index    int
}

var maptasks chan MapTask
var reducetasks chan int

type Master struct {
	mapTaskStatus     map[string]int
	reduceTaskStatus  map[int]int
	finish            bool
	inputFiles        []string
	nReduce           int
	mapIndex          int
	reduceIndex       int
	intermediateFiles [][]string
	RWMutexLock       *sync.RWMutex
	mapFinished       bool
	reduceFinished    bool
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) DistributeTask(args *MrArgs, reply *MrReply) error {
	select {
	case mapTask := <-maptasks:
		reply.MapFileName = mapTask.filename
		reply.Index = mapTask.index
		reply.TaskType = "map"
		reply.NReduce = m.nReduce
		m.RWMutexLock.Lock()
		m.mapTaskStatus[mapTask.filename] = Started
		m.RWMutexLock.Unlock()
		go m.watchWorkerMap(mapTask)
		return nil
	case reduceNumber := <-reducetasks:
		reply.Files = m.intermediateFiles[reduceNumber]
		reply.Index = reduceNumber
		reply.TaskType = "reduce"
		m.RWMutexLock.Lock()
		m.reduceTaskStatus[reduceNumber] = Started
		m.RWMutexLock.Unlock()
		go m.watchWorkerReduce(reduceNumber)
		return nil
	}
	return nil
}

func (m *Master) watchWorkerMap(task MapTask) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.RWMutexLock.Lock()
			m.mapTaskStatus[task.filename] = NotStarted
			m.RWMutexLock.Unlock()
			maptasks <- task
		default:
			m.RWMutexLock.RLock()
			if m.mapTaskStatus[task.filename] == Finished {
				m.RWMutexLock.RUnlock()
				return
			}
			m.RWMutexLock.RUnlock()
		}
	}
}

func (m *Master) watchWorkerReduce(reduceNumber int) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.RWMutexLock.Lock()
			m.reduceTaskStatus[reduceNumber] = NotStarted
			m.RWMutexLock.Unlock()
			reducetasks <- reduceNumber
		default:
			m.RWMutexLock.RLock()
			if m.reduceTaskStatus[reduceNumber] == Finished {
				m.RWMutexLock.RUnlock()
				return
			}
			m.RWMutexLock.RUnlock()
		}
	}
}

func (m *Master) NotifyIntermediateFile(args *NotifyIntermediateArgs, reply *NotifyReply) error {
	m.RWMutexLock.Lock()
	defer m.RWMutexLock.Unlock()
	m.intermediateFiles[args.ReduceIndex] = append(m.intermediateFiles[args.ReduceIndex], args.File)
	return nil
}

func (m *Master) NotifyMapSuccess(args *NotifyMapSuccessArgs, reply *NotifyReply) error {
	m.RWMutexLock.Lock()
	defer m.RWMutexLock.Unlock()
	m.mapTaskStatus[args.File] = Finished
	finished := true
	for _, v := range m.mapTaskStatus {
		if v != Finished {
			finished = false
			break
		}
	}
	m.mapFinished = finished
	if m.mapFinished {
		for i := 0; i < m.nReduce; i++ {
			m.reduceTaskStatus[i] = NotStarted
			reducetasks <- i
		}
	}
	return nil
}

func (m *Master) NotifyReduceSuccess(args *NotifyReduceSuccessArgs, reply *NotifyReply) error {
	m.RWMutexLock.Lock()
	defer m.RWMutexLock.Unlock()
	m.reduceTaskStatus[args.ReduceIndex] = Finished
	finished := true
	for _, v := range m.reduceTaskStatus {
		if v != Finished {
			finished = false
			break
		}
	}
	m.reduceFinished = finished
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.RWMutexLock.Lock()
	defer m.RWMutexLock.Unlock()
	ret := m.reduceFinished
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	maptasks = make(chan MapTask, len(files))
	reducetasks = make(chan int, nReduce)
	m.mapTaskStatus = make(map[string]int, len(files))
	m.reduceTaskStatus = make(map[int]int, nReduce)
	for index, file := range files {
		m.mapTaskStatus[file] = NotStarted
		mapTask := MapTask{}
		mapTask.index = index
		mapTask.filename = file
		maptasks <- mapTask
	}

	m.inputFiles = files
	m.nReduce = nReduce
	m.intermediateFiles = make([][]string, nReduce)
	m.RWMutexLock = new(sync.RWMutex)
	m.server()
	return &m
}
