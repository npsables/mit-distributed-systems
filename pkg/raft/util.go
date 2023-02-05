package raft

import (
	"fmt"
	"os"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if os.Getenv("DEBUG") == "1" {
		fmt.Printf(format, a...)
	}
	return
}
