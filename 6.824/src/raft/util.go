package raft

import "log"

// Debugging
const Debug = 1
const Debug_Rejoin = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func DPrintf_Rejoin(format string, a ...interface{}) (n int, err error) {
	if Debug_Rejoin > 0 {
		log.Printf(format, a...)
	}
	return
}
