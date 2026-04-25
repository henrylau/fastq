package main

import (
	"context"
	"fmt"
	"time"

	"github.com/henrylau/fastq"
)

func main() {
	start := time.Now()

	handler := func(ctx context.Context, n int) (int, error) {
		time.Sleep(50 * time.Millisecond)
		return n * n, nil
	}

	q := fastq.NewFastQueue(context.Background(), handler, 2)

	results := make([]int, 10)
	for i := 0; i < 10; i++ {
		q.PushWithCallback(context.Background(), i, func(ctx context.Context, result int, err error) {
			if err != nil {
				fmt.Println("task", i, "failed with error:", err)
				return
			}
			results[i] = result
			fmt.Println("task", i, "completed in", time.Since(start))
		})
	}
	fmt.Println("pushed all tasks", time.Since(start))

	q.WaitEmpty()

	fmt.Println("results:", results)
}
