package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debug Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func randomDuration(min time.Duration, max time.Duration) time.Duration {
	minNs := int64(min)
	maxNs := int64(max)
	randomNs := rand.Int63n(maxNs-minNs) + minNs
	return time.Duration(randomNs)
}
