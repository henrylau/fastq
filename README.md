# fastq

A small, generic worker queue for Go. Bound concurrency to a fixed number of workers, push tasks synchronously or fire-and-forget, pause/resume, and shut down cleanly without losing in-flight work.


```
go get github.com/henrylau/fastq
```

## Why

Bare goroutines have no concurrency cap. Custom semaphore patterns work but leak observability — you can't ask "how many tasks are pending?" or "pause new work for 30 seconds." `fastq` gives you a typed, bounded worker pool with first-class lifecycle controls in ~300 lines.

## Quick start

```go
package main

import (
    "context"
    "fmt"

    "github.com/henrylau/fastq"
)

func main() {
    // Workers run this handler. Type parameters: input=int, output=string.
    handler := func(ctx context.Context, n int) (string, error) {
        return fmt.Sprintf("squared=%d", n*n), nil
    }

    q := fastq.NewFastQueue(context.Background(), handler, 4) // 4 workers
    defer q.Stop()

    // Synchronous push — blocks until the worker returns.
    result, err := q.Push(context.Background(), 7, nil)
    if err != nil {
        panic(err)
    }
    fmt.Println(result) // squared=49
}
```

## Usage

### Synchronous push

`Push` queues a task and blocks until the worker returns its result.

```go
out, err := q.Push(ctx, payload, nil)
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

Pass a callback to `Push` and it runs in its own goroutine after the result is produced. The synchronous return value still fires — the callback is additive.

```go
cb := func(ctx context.Context, result string, err error) {
    if err != nil {
        log.Printf("task failed: %v", err)
        return
    }
    metrics.Record(result)
}
_, _ = q.Push(ctx, payload, cb)
```

### Pause and resume

Workers stop picking up new tasks once `Pause()` returns. In-flight handlers run to completion. `Resume()` wakes them up.

```go
q.Pause()
// ... do something while no new tasks start ...
q.Resume()
```

### Per-task context

If `ctx` passed to `Push` is non-nil, the handler receives a context derived from it (and cancelled if the queue stops). This lets the caller carry deadlines, tracing IDs, etc.

```go
ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()
result, err := q.Push(ctx, payload, nil)
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

`Clear()` drops everything currently in the input buffer (delivering `ErrTaskCancelled` to any waiting `Push` callers). Tasks already being executed by a worker continue.

```go
q.Clear()
```

### Shutdown

`Stop()` is the graceful shutdown:

1. Rejects new `Push` calls (`ErrQueueStopped`).
2. Cancels each worker's task context, so handlers that respect `ctx` exit early.
3. Drains any tasks still in the buffer, delivering `ErrTaskCancelled` to their result channels.
4. Returns once all workers and pending pushes have settled.

`Stop()` is safe to call multiple times and from any goroutine.

```go
q.Stop()
```

## Error semantics

| Error | When |
|---|---|
| `ErrQueueStopped` | `Push` / `PushWithoutResult` called after `Stop()` started and admission was rejected. |
| `ErrTaskCancelled` | Task was admitted but never executed (cancelled by `Stop` or `Clear`), or the handler returned a non-nil error after queue shutdown. |
| handler error | Whatever the handler returned. |

`Push` returns the task's *fate*: a real result, or one of the errors above.

## Performance

Benchmarks on Apple M2 Pro (Go 1.25, 12 cores), comparing against a hand-written semaphore-bounded goroutine pattern with the same concurrency cap:

| | ns/op | allocs/op |
|---|---|---|
| Bare goroutine (unbounded) | ~320 | 2 |
| Semaphore-bounded goroutine | ~420–620 | 2 |
| `PushWithoutResult` | ~640–1300 | 4 |
| `Push` (with result channel) | ~1700–2200 | 8–9 |

The overhead pays for: pause/resume, stats, cancellable in-flight tasks, graceful `Stop`, and per-task callbacks. For real workloads (≥10 µs per task) the coordination cost is noise.

Run them yourself:

```
go test -bench=. -benchmem -run=^$ ./...
```

## API summary

```go
func NewFastQueue[T, R any](ctx context.Context,
    handler func(context.Context, T) (R, error),
    numberOfWorkers int) *FastQueue[T, R]

func (q *FastQueue[T, R]) Push(ctx context.Context, payload T, cb Callback[R]) (R, error)
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

## License

MIT.
