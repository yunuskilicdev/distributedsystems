package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for true {
		reply := CallMaster()
		if reply.TaskType == "map" {
			executeMap(mapf, reply)
		} else if reply.TaskType == "reduce" {
			executeReduce(reducef, reply)
		} else {
			break
		}
	}
}

func executeReduce(reducef func(string, []string) string, reply MrReply) {
	intermediate := []KeyValue{}
	for _, v := range reply.Files {
		file, err := os.Open(v)
		if err != nil {
			log.Fatalf("cannot open %v", v)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(reply.Index)
	ofile, _ := os.Create(oname)

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
	}
	NotifyReduceSuccess(reply.Index)
}

func executeMap(mapf func(string, string) []KeyValue, reply MrReply) {
	fileName := reply.MapFileName
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()

	kva := mapf(fileName, string(content))
	kvap := ArrangeIntermediate(kva, reply.NReduce)
	files := []string{}
	for i := range kvap {
		values := kvap[i]
		filename := "mr-" + strconv.Itoa(reply.Index) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(filename)

		enc := json.NewEncoder(ofile)
		for _, kv := range values {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal("error: ", err)
			}
		}
		files = append(files, filename)
		NotifyMaster(i, filename)
		ofile.Close()
	}
	NotifyMapSuccess(fileName)
}

func NotifyMapSuccess(filename string) {
	args := NotifyMapSuccessArgs{}
	args.File = filename
	reply := NotifyReply{}
	call("Master.NotifyMapSuccess", &args, &reply)
}

func NotifyReduceSuccess(reduceIndex int) {
	args := NotifyReduceSuccessArgs{}
	args.ReduceIndex = reduceIndex
	reply := NotifyReply{}
	call("Master.NotifyReduceSuccess", &args, &reply)
}

func ArrangeIntermediate(kva []KeyValue, nReduce int) [][]KeyValue {
	kvap := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		v := ihash(kv.Key) % nReduce
		kvap[v] = append(kvap[v], kv)
	}
	return kvap
}

func CallMaster() MrReply {
	args := MrArgs{}
	reply := MrReply{}
	call("Master.DistributeTask", &args, &reply)
	return reply
}

func NotifyMaster(reduceIndex int, file string) {
	args := NotifyIntermediateArgs{}
	args.ReduceIndex = reduceIndex
	args.File = file
	reply := NotifyReply{}
	call("Master.NotifyIntermediateFile", &args, &reply)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}
