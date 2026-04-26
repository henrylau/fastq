// This example wraps an arbitrary function with a FastQueue so that no
// matter how many goroutines call the wrapped function, at most N
// invocations run concurrently. It's a drop-in concurrency limiter.
//
// The queue's worker is the original function; callers use PushAndWait,
// which blocks until their slot runs and returns the function's result.
package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/henrylau/fastq"
)

type ApiParams struct {
	param1 string
	param2 string
}

type ApiResponse struct {
	result string
}

func apiCall(ctx context.Context, task ApiParams) (ApiResponse, error) {
	// simulate api call
	time.Sleep(1 * time.Second)

	return ApiResponse{result: "success"}, nil
}

const maxConcurrent = 2

// Wrap the api call with a FastQueue so that no matter how many goroutines call the wrapped function,
// at most maxConcurrent invocations run concurrently.
var ApiCall = fastq.NewFastQueue(context.Background(), apiCall, maxConcurrent)

func main() {
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(10)

	for i := 0; i < 10; i++ {
		go func(i int) {
			defer waitGroup.Done()
			res, err := ApiCall.PushAndWait(context.Background(), ApiParams{param1: fmt.Sprintf("param1 %d", i)})

			if err != nil {
				log.Printf("task %d error: %v\n", i, err)
				return
			}

			log.Printf("task %d result: %s\n", i, res.result)
		}(i)
	}

	// wait for all tasks enqueued
	waitGroup.Wait()

	// wait for all tasks completed
	ApiCall.WaitEmpty()
}
