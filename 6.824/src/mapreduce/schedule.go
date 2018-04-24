package mapreduce

import (
    "fmt"
    "sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

    fmt.Println("schedule()-",phase)
    // make a task index channel
    var wg sync.WaitGroup
    ch_taskindex := make(chan int, ntasks)
    for i := 0; i < ntasks; i++ {
        ch_taskindex <- i
        wg.Add(1)
    }

    fmt.Println("schedule()-all the task indexes are written into the channel!")
    // create a goroutine to get worker address from registerChan
    go func() {
        for {
            worker_addr := <-registerChan
            go func() {
                for {
                    // read from the ch_taskindex
                    task_i := <-ch_taskindex
                    // construct a DoTaskArgs
                    args := new(DoTaskArgs)
                    args.JobName = jobName
                    switch phase {
                    case mapPhase: args.File = mapFiles[task_i]
                    case reducePhase: args.File = "" // doesn't matter.
                    }
                    args.Phase = phase
                    args.TaskNumber = task_i
                    args.NumOtherPhase = n_other
                    // RPC call: DoTask
                    ok := call(worker_addr, "Worker.DoTask", args, new(struct{}))
                    if ok == false {
                        fmt.Println("schedule()-fail to DoTask!")
                        ch_taskindex <- task_i   // fault-tolerant!
                    } else {
                        wg.Done()
                    }
                }
            }()
        }
    }()

    // wait for all the tasks to be completed.
    wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
