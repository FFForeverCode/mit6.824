package raft

import (
	"log"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func sleepRandom(ms int64) {
	time.Sleep(time.Duration(ms) * time.Millisecond)
}