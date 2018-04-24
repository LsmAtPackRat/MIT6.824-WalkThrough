package mapreduce

import (
    "os"
    "sort"
    "encoding/json"
    //"io/ioutil"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

    // we should read nMap intermediate file. xxx-0-reduceTask.txt ~ xxx-(nMap-1)-reduceTask.txt, and decode the file content, and save them into a big map
    var k2vs = make(map[string][]string)
    for m := 0; m < nMap; m++ {
        intermediate_filename := reduceName(jobName, m, reduceTask)
        file, err := os.Open(intermediate_filename)
        if err != nil {
            debug("doReduce()-fail to open intermediate file!")
        }
        dec := json.NewDecoder(file)
        for {
            // Decode will get a key to values map
            var temp = make(map[string][]string)
            err = dec.Decode(&temp)
            if err != nil {
                break
            }
            // append all the k/v pairs into k2vs
            for key, values := range temp {
                k2vs[key] = append(k2vs[key], values...)
            }
        }
        file.Close()
    }

    // sort keys in k2vs
    var keys []string
    for key, _ := range k2vs {
        keys = append(keys, key)
    }
    sort.Strings(keys)

    // now we will call reduceF on each key in k2vs
    p := mergeName(jobName, reduceTask)  // result_file's name
    result_file, err := os.Create(p)
    if err != nil {
        debug("doReduce()-fail to create result_file!")
    }
    enc := json.NewEncoder(result_file)
    for _, key := range keys {
        result := reduceF(key, k2vs[key])
        enc.Encode(&KeyValue{key, result})
    }
    result_file.Close()
}










