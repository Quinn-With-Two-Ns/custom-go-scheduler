# Deterministic execution bubbles: research

Researched 2026-09-16 from primary sources and direct source inspection. Earlier
project designs are not treated as evidence.

## Findings

Extending Go's existing bubble machinery is a credible starting point. It already
propagates membership through `go`, associates channels with bubbles, detects
quiescence, and virtualizes time. It does **not** implement deterministic
scheduling or comprehensive isolation. A Temporal integration additionally needs
an ordered, resumable boundary between history processing and workflow execution.

The recommended first experiment is a deterministic, host-driven Go bubble with
native `go`, channels, and `select`, exercised by a generic recorded-input harness.
Native host-driven timers and standard-library compatibility follow as explicit
stages. Scope is Go language implementation, runtime, and standard-library changes
only. A future Temporal SDK is a consumer, not something designed here.
This is a feasibility recommendation, not a claim that implementation is easy.

## Source baselines

| Source | Inspected version |
| --- | --- |
| Local Go checkout, `go/` from the workspace root | `e2f9f2745d43da0b1b3d6f1953a7505bc95636b0`, 2026-09-16 |
| Local Temporal Go SDK, `../../../sdk-go/` from this document | `3f2850f35a44dac50230f867e738cec2f88aeb7f`, 2026-09-08 |
| Installed Go used for the baseline experiment | `go1.27.0 darwin/arm64` |

The source references below refer to those revisions, not a promise about future
releases. The Go source checkout and SDK checkout were clean when inspected.
Public package documentation was also checked, but local implementation details
are distinguished from public API guarantees.

## What Temporal needs

Temporal reconstructs workflow state by replaying history and checking workflow
commands against historical events. The relevant observable contract is command
compatibility given the same history, not identical CPU instructions or persisted
goroutine stacks. A scheduler seed alone cannot supply this contract.
[Temporal workflow definition documentation](https://docs.temporal.io/workflow-definition),
[SDK command matching](https://github.com/temporalio/sdk-go/blob/3f2850f35a44dac50230f867e738cec2f88aeb7f/internal/internal_task_handlers.go).

| Requirement | Implication for a bubble |
| --- | --- |
| Reconstruct state after worker loss | Recreate the bubble and replay ordered inputs; do not serialize Go stacks. |
| Run workflow code at workflow-task boundaries | Apply the SDK's event batch, then execute until no workflow goroutine can progress. Do not run user code immediately from network callbacks. |
| Wait for activities, signals, and durable timers | Quiescence is usually suspension, not deadlock. A host can provide future inputs. |
| Preserve command order | Specify runnable order, channel wakeups, select choices, and event-delivery order. Retain the SDK's command state machines and replay checks. |
| Logical time | Advance the workflow clock from the SDK's replay time. A timer must not fire merely because the host clock passes its deadline. |
| Cancellation | Distinguish recorded workflow cancellation from worker shutdown/cache eviction. Only the former is a workflow input. |
| Queries and updates | Eventually support query nonblocking/read-only constraints, update validation and scheduling, and signal-handler ordering. These are more than goroutine launch details. |
| Failure and diagnostics | Attribute blocked stacks and non-yielding execution to a bubble; distinguish a failed workflow task from workflow failure. |
| Cache eviction | Discard suspended computation and release resources without producing workflow commands. This is a first-class lifecycle problem. |
| Long-lived histories | Pin or version scheduling semantics; a new deterministic schedule can still disagree with an old history. |

The SDK's `WorkflowDefinition` contract is particularly useful: history callbacks
schedule work; application execution occurs in `OnWorkflowTaskStarted` after the
event batch. It also requires `StackTrace` and a `Close` that destroys coroutines.
[Source, lines 168–187](https://github.com/temporalio/sdk-go/blob/3f2850f35a44dac50230f867e738cec2f88aeb7f/internal/internal_worker_base.go#L168).

The current dispatcher runs coroutines until they block and has additional
scheduling rules. Workflow channels and context access are coupled to that
dispatcher. Putting an unchanged `workflow.Context` workflow inside a bubble
does not replace this machinery.
[Dispatcher](https://github.com/temporalio/sdk-go/blob/3f2850f35a44dac50230f867e738cec2f88aeb7f/internal/internal_workflow.go#L1281),
[workflow implementation](https://github.com/temporalio/sdk-go/blob/3f2850f35a44dac50230f867e738cec2f88aeb7f/internal/internal_workflow.go#L331).

This research uses SDK internals solely to establish consumer requirements.
SDK integration points, workflow APIs, command translation, activity helpers,
and migration implementation are outside the Go design in this workspace.

## What the Go checkout already supplies

Paths below are relative to `go/` and are direct implementation evidence.

| Capability | Source | Limitation |
| --- | --- | --- |
| Child goroutine inheritance | `src/runtime/proc.go:5401`; `src/runtime/runtime2.go:573` | Runtime system goroutines are separate. |
| Bubble activity and quiescence accounting | `src/runtime/synctest.go:25`, `:43`; `src/runtime/proc.go:4274` | External waits count as active; they are not generally rejected. Preserve transient park accounting. |
| Channel association | `src/runtime/chan.go:116`, `:185`, `:410`, `:532` | Existing checks stop outsiders using bubbled channels, but do not generally stop insiders using ordinary channels. |
| WaitGroup association | `src/sync/waitgroup.go:87`, `:192` | Nonheap/package-variable association has limitations: `src/runtime/synctest.go:379`. |
| Virtual clock and timer heap | `src/runtime/time.go:16`, `:696` | Clock readings have no monotonic component; timer behavior still needs a durable-host policy. |
| Auto-advance at quiescence | `src/runtime/synctest.go:202` | Synctest advances to the next timer; Temporal must wait for recorded events instead. |
| Internal bubble entry mechanism | `src/internal/synctest/synctest.go:71` | Acquire/Run/Release is not an ordered, exclusive host activation protocol. |
| Race-detector integration for waiting | `src/runtime/synctest.go:281` | A new API must specify its own synchronization edges without hiding arbitrary races. |

These facts agree with the public distinction between durable blocking and
ordinary blocking. Mutexes, network I/O, and syscalls are not durably blocking in
synctest. Finalizers and runtime cleanups run outside bubbles.
[Public synctest documentation](https://pkg.go.dev/testing/synctest).

## Missing determinism and isolation

1. **Runnable scheduling.** `newproc` (`proc.go:5335`) and `ready` (`:1133`)
   use ordinary scheduler queues. Yield/preemption (`:4323`), canceled parking
   (`:4269`), and direct coroutine switches (`coro.go:140`) are additional paths.
   A per-bubble logical execution permit must cover all of them. Setting
   `GOMAXPROCS=1` is neither sufficient nor an acceptable process-wide API.
2. **Select choices.** `runtime/select.go:191` uses runtime randomness. A
   reproducible, bubble-local PRNG and defined consumption rules are needed.
   Always choosing the first ready case would conflict with the specification's
   uniform pseudo-random selection rule. Compiler-lowered single-case/default
   selects and `reflect.Select` need coverage too.
   [Language specification](https://go.dev/ref/spec#Select_statements).
3. **Timer ties.** `runtime/time.go:173` and `:712` deliberately randomize
   equal-deadline fake timers. They require explicit deterministic ordering or a
   separate seeded choice stream, plus recorded delivery for durable workflows.
4. **Preemption.** GC and runtime preemption must remain possible. An arbitrary
   physical preemption must not give a different workflow goroutine the logical
   turn. Disabling all preemption would endanger runtime health and diagnostics.
   See `runtime/preempt.go:7` and `runtime/proc.go:1432`.
5. **Synchronization ownership.** Mutex starvation uses elapsed real time
   (`internal/sync/mutex.go:147`, `runtime/sema.go:733`). Cond ownership checks
   protect queued waiters, not a complete object boundary (`sema.go:656`, `:713`).
   Timer Stop/Reset check for some bubble, whereas timer-channel access checks
   exact bubble identity (`time.go:422`, `:437`, `:1413`). These need auditing.
6. **Memory and other inputs.** Bubbles do not own arbitrary memory. Outside
   writes, global state, atomics, process configuration, I/O, randomness,
   finalizers, and map-dependent decisions can still change results. Isolation
   of synchronization objects is not a memory sandbox.

An important standard-library example is `context.cancelCtx.cancel`: it ranges
over a map of children at `src/context/context.go:574`. That can vary cancellation
and wakeup order. There is also a shared preclosed Done channel at `:570`.
Consequently, even a deterministic scheduler plus native timers does not
automatically make all ordinary `context` usage replay-safe. Similar library
audits are necessary before promising a broadly native experience.

The MVP should support data-race-free programs within an explicit restrictions
list. Serial execution alone does not settle Go memory-model or race-detector
semantics for unsynchronized shared workflow state.

## Empirical baseline

A temporary test ran 100 fresh `synctest.Test` bubbles under installed Go 1.27.0.
Each selected between two ready buffered channels, then collected two goroutines
sleeping for the same virtual duration:

```text
ready-select outcomes:       1:48, 2:52
equal-deadline wake outcomes: AB:52, BA:48
PASS
```

This confirms multiple observed outcomes for identical test code; it is not a
statistical proof, a performance benchmark, or a test of a modified runtime.
The local development checkout has not been built. Reproduction body:

```go
choices := map[int]int{}
orders := map[string]int{}
for i := 0; i < 100; i++ {
    synctest.Test(t, func(t *testing.T) {
        a, b := make(chan int, 1), make(chan int, 1)
        a <- 1
        b <- 2
        select {
        case v := <-a:
            choices[v]++
        case v := <-b:
            choices[v]++
        }
        out := make(chan string, 2)
        go func() { time.Sleep(time.Second); out <- "A" }()
        go func() { time.Sleep(time.Second); out <- "B" }()
        orders[<-out + <-out]++
    })
}
t.Log(choices, orders)
```

## Prior proposals and the path upstream

[Go issue #33702](https://github.com/golang/go/issues/33702) proposed deterministic
execution for Cadence in 2019. Maintainers challenged isolation and specification
details. The closing response requested an out-of-tree implementation that
demonstrates limited runtime impact before a concrete proposal. It also raised
maintenance burden and the difficulty of determinizing pointer-keyed maps.
[Closing response](https://github.com/golang/go/issues/33702#issuecomment-523179911).

A 2026 deterministic concurrency-testing proposal was likewise closed as
non-actionable without sufficient design and implementation.
[Issue #80316](https://github.com/golang/go/issues/80316#issuecomment-4923638253).
Synctest's own work deliberately randomized timer ties rather than making its
schedules a replay contract.
[Issue #73850](https://github.com/golang/go/issues/73850).

Our stronger starting point is existing bubble infrastructure and a narrower
claim: reproducible scheduling for constrained, isolated code with explicit host
inputs. We should preserve synctest's testing semantics and prototype an opt-in
policy over shared machinery. Do not present a Temporal-specific scheduler as a
required policy for every Go program.

A future proposal should contain the working fork, precise semantics, measured
cost outside bubbles, replay examples, lifecycle behavior, compatibility strategy,
and explicit unsupported operations. A second use case, such as deterministic
simulation, would test whether the API is sufficiently general. Acceptance is
uncertain. The normal Go process starts with an issue and may request a design
document; the implementation-first recommendation here comes from the feedback
on this particular feature family.
[Go proposal process](https://go.googlesource.com/proposal/+/master/README.md).
