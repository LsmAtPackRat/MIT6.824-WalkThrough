package raft

import "log"
import "fmt"

var RV_RPCS int
var AE_RPCS int
var RPC_REPORTED bool

// Debugging
const Debug = 1
const Sebug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func SPrintf(format string, a ...interface{}) (n int, err error) {
	if Sebug > 0 {
		log.Printf(format, a...)
	}
	return
}


func ResetStatistics() {
	RV_RPCS = 0
	AE_RPCS = 0
	RPC_REPORTED = false
}

func PrintStatistics() {
	if !RPC_REPORTED {
		fmt.Printf("send %d RequestVote RPC, send %d AppendEntries RPC.\n\n", RV_RPCS, AE_RPCS)
		RPC_REPORTED = true
	}
}

func TDPrintf(format string, a ...interface{}) (n int, err error) {
	log.Printf(format, a...)
	return
}
