#!/bin/bash
# rm reduce*.txt
go build -race -buildmode=plugin ../apps/wc/wc.go
go run -race mrworker.go wc.so
