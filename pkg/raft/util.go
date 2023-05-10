package raft

import (
	"fmt"
	"os"
	"time"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if os.Getenv("DEBUG") == "1" {
		fmt.Printf(time.Now().Format("15:04:05")+"    "+format+"\n", a...)
		return
	}
	return
}
