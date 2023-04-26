package raft

import "log"

// Debugging
// const Debug := true
// const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	Debug := true
	Debug = false
	if Debug {
		log.SetFlags(log.Lmicroseconds)
		log.Printf(format, a...)
	}
	return
}
