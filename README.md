# fastq

[![Build and Test](https://github.com/henrylau/fastq/actions/workflows/ci.yml/badge.svg)](https://github.com/henrylau/fastq/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/henrylau/fastq/branch/main/graph/badge.svg)](https://codecov.io/gh/henrylau/fastq)
[![Go Report Card](https://goreportcard.com/badge/github.com/henrylau/fastq)](https://goreportcard.com/report/github.com/henrylau/fastq)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A small, generic worker queue for Go. Bound concurrency to a fixed number of workers, push tasks synchronously or fire-and-forget, pause/resume, and shut down cleanly without losing in-flight work.


```
go get github.com/henrylau/fastq
```

## Why

Bare goroutines have no concurrency cap. Custom semaphore patterns work but leak observability — you can't ask "how many tasks are pending?" or "pause new work for 30 seconds." `fastq` gives you a typed, bounded worker pool with first-class lifecycle controls in ~300 lines.

## Quick start

Push 10 tasks into a pool of 4 workers. Each `PushAndWait` call blocks until
its own task returns, but workers run in parallel — so the concurrency cap is
enforced regardless of how many callers are pushing.

```go
package main

import (
    "context"
    "fmt"
    "sync"
    "sync/atomic"
    "time"

    "github.com/henrylau/fastq"
)

func main() {
    var inflight, peak atomic.Int32

    handler := func(ctx context.Context, n int) (int, error) {
        cur := inflight.Add(1)
        defer inflight.Add(-1)

        // Track the peak number of handlers running at once.
        for p := peak.Load(); cur > p; p = peak.Load() {
            if peak.CompareAndSwap(p, cur) {
                break
            }
        }

        time.Sleep(50 * time.Millisecond) // simulate real work
        return n * n, nil
    }

    // Cap concurrency at 4 workers.
    q := fastq.NewFastQueue(context.Background(), handler, 4)
    defer q.Stop()

    const total = 10
    results := make([]int, total)

    var wg sync.WaitGroup
    wg.Add(total)
    for i := 0; i < total; i++ {
        go func(i int) {
            defer wg.Done()
            // PushAndWait blocks until this task's worker returns.
            r, _ := q.PushAndWait(context.Background(), i)
            results[i] = r
        }(i)
    }
    wg.Wait()

    fmt.Println("results:", results)
    fmt.Println("peak concurrent handlers:", peak.Load(), "(cap=4)")
    // results: [0 1 4 9 16 25 36 49 64 81]
    // peak concurrent handlers: 4 (cap=4)
}
```

Even though 10 goroutines called `PushAndWait` at the same moment, at most 4
handlers ever ran in parallel. Each caller still got its own result back
synchronously.

## Usage

### Synchronous push

`PushAndWait` queues a task and blocks until the worker returns its result.

```go
out, err := q.PushAndWait(ctx, payload)
```

### Fire-and-forget

`PushWithoutResult` queues a task and returns immediately. Use `WaitEmpty()` to wait for everything to drain.

```go
for _, item := range items {
    if err := q.PushWithoutResult(ctx, item); err != nil {
        log.Printf("queue refused task: %v", err)
    }
}
q.WaitEmpty() // blocks until all queued tasks have completed
```

### Callback after completion

`PushWithCallback` queues a task and returns immediately. The callback runs in
its own goroutine once the worker finishes — use it when you want the result
without blocking the caller.

```go
cb := func(ctx context.Context, result string, err error) {
    if err != nil {
        log.Printf("task failed: %v", err)
        return
    }
    metrics.Record(result)
}
if err := q.PushWithCallback(ctx, payload, cb); err != nil {
    log.Printf("queue refused task: %v", err)
}
```

Tasks pushed via `PushWithCallback` and `PushWithoutResult` are enqueued in
the order their `Push*` calls returned, even though the actual sends happen on
internal goroutines.

### Concurrency-limited function wrapper

A queue with the function as its handler turns the function into a
concurrency-limited callable: no matter how many goroutines call it, at
most N invocations run in parallel. Callers use `PushAndWait`, which
blocks until their slot runs and returns the function's result.

```go
type ApiParams struct {
    param1 string
    param2 string
}

type ApiResponse struct {
    result string
}

func apiCall(ctx context.Context, task ApiParams) (ApiResponse, error) {
    // ... real work, e.g. HTTP request ...
    time.Sleep(1 * time.Second)
    return ApiResponse{result: "success"}, nil
}

const maxConcurrent = 2

// At most 2 apiCall invocations run in parallel, regardless of caller count.
var ApiCall = fastq.NewFastQueue(context.Background(), apiCall, maxConcurrent)

func main() {
    var wg sync.WaitGroup
    wg.Add(10)
    for i := 0; i < 10; i++ {
        go func(i int) {
            defer wg.Done()
            res, err := ApiCall.PushAndWait(context.Background(),
                ApiParams{param1: fmt.Sprintf("param1 %d", i)})
            if err != nil {
                log.Printf("task %d error: %v", i, err)
                return
            }
            log.Printf("task %d result: %s", i, res.result)
        }(i)
    }
    wg.Wait()
    ApiCall.WaitEmpty()
}
```

Full runnable version: [`examples/concurrent-limit`](examples/concurrent-limit).

### Pause and resume

Workers stop picking up new tasks once `Pause()` returns. In-flight handlers run to completion. `Resume()` wakes them up.

```go
q.Pause()
// ... do something while no new tasks start ...
q.Resume()
```

### Per-task context

If `ctx` passed to a `Push*` call is non-nil, the handler receives a context
derived from it (and cancelled if the queue stops). This lets the caller carry
deadlines, tracing IDs, etc.

```go
ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()
result, err := q.PushAndWait(ctx, payload)
```

### Inspecting state

```go
stats := q.Status()
// stats.Status:    Running | Paused | Ready | Stopped
// stats.Pending:   tasks queued or in-flight
// stats.Completed: handlers that returned
// stats.Failed:    handlers that returned an error (or were cancelled)

q.Pending()   // shortcut for stats.Pending
q.Completed() // ...
q.Failed()    // ...
```

### Discarding pending work

`Clear()` drops everything currently in the input buffer (delivering `ErrTaskCancelled` to any waiting `PushAndWait` callers and to `PushWithCallback` callbacks). Tasks already being executed by a worker continue.

```go
q.Clear()
```

### Shutdown

`Stop()` is the graceful shutdown:

1. Rejects new `Push*` calls (`ErrQueueStopped`).
2. Cancels each worker's task context, so handlers that respect `ctx` exit early.
3. Drains any tasks still in the buffer, delivering `ErrTaskCancelled` to their result channels (or callbacks).
4. Returns once all workers and pending pushes have settled.

`Stop()` is safe to call multiple times and from any goroutine.

```go
q.Stop()
```

## Error semantics

| Error | When |
|---|---|
| `ErrQueueStopped` | Any `Push*` called after `Stop()` started and admission was rejected. |
| `ErrTaskCancelled` | Task was admitted but never executed (cancelled by `Stop` or `Clear`). Surfaced via the return of `PushAndWait` or the `err` argument of a `PushWithCallback` callback. |
| handler error | Whatever the handler returned. |

`PushAndWait` returns the task's *fate*: a real result, or one of the errors above. `PushWithCallback` and `PushWithoutResult` only return `ErrQueueStopped` from admission — task-level errors arrive via the callback or are silently counted as `Failed`.


## API summary

```go
func NewFastQueue[T, R any](ctx context.Context,
    handler func(context.Context, T) (R, error),
    numberOfWorkers int) *FastQueue[T, R]

func (q *FastQueue[T, R]) PushAndWait(ctx context.Context, payload T) (R, error)
func (q *FastQueue[T, R]) PushWithCallback(ctx context.Context, payload T, cb Callback[R]) error
func (q *FastQueue[T, R]) PushWithoutResult(ctx context.Context, payload T) error

func (q *FastQueue[T, R]) WaitEmpty()
func (q *FastQueue[T, R]) Pause()
func (q *FastQueue[T, R]) Resume()
func (q *FastQueue[T, R]) Clear()
func (q *FastQueue[T, R]) Stop()

func (q *FastQueue[T, R]) Pending()   int
func (q *FastQueue[T, R]) Completed() int
func (q *FastQueue[T, R]) Failed()    int
func (q *FastQueue[T, R]) Status()    QueueStats
```

## AI assistance

Parts of this project's documentation, test cases, and examples were drafted
with the help of an AI assistant. Every AI-drafted change was reviewed,
edited, and accepted by the maintainer before being committed; the core
library code is human-authored.

## License

MIT.
