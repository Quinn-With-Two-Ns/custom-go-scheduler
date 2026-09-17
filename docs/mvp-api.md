# MVP API: a deterministic, host-driven Go bubble

This is a proposed experiment, not existing Go API. The package name
`runtime/bubble` is a placeholder for a package available only in the fork.
It should reuse and extend runtime bubble machinery while preserving
`testing/synctest` behavior. [Research and evidence](research.md).

Implementation update: the fork now implements New/Step, independent clocks,
native concurrency, host timers, and configurable application randomness. See
[current implementation and limits](implementation.md).
The design below remains the broader target; Run, disposal, and general panic
containment are not implemented.

Scope is Go runtime, compiler integration where necessary, and standard-library
behavior. No new Go syntax is proposed. Temporal supplies requirements for a
future consumer; no SDK API, adapter, command mapping, or worker is designed here.

## Scope and determinism contract

Start with one fixed cooperative scheduling policy. Users opt in by running a
function with options; descendants inherit membership through ordinary `go`.
Arbitrary user-supplied scheduler callbacks are outside this first experiment.

For a supported, data-race-free program, the same binary, runtime policy, seed,
initial state, and ordered activations must produce the same observable bubble
outputs. Unrelated host load, `GOMAXPROCS`, GC timing, and physical preemption
must not alter that result. Cross-build, cross-architecture, and cross-release
replay are separate compatibility claims that require evidence.

Supported first: `go`, owned buffered/unbuffered channels, close, range over
channels, native `select`, explicit yields, host-supplied `time.Now`, and repeated
suspension/resumption. Host-driven native `time.Sleep` and `time.NewTimer` are
required for the completed Go MVP, built after the concurrency core. AfterFunc,
Ticker, context, and general sync compatibility are separate extensions. Until
timer support lands, native timer use must fail explicitly rather than silently
use synctest's automatic clock advancement.

Restrictions remain on map-dependent observable ordering, shared mutable state
outside the bubble, uncontrolled randomness (including crypto/rand), I/O, cgo,
unsafe escapes, and finalizer
effects. Native mutex/Cond/WaitGroup/context compatibility is not promised until
each operation has an ownership and ordering audit. These restrictions also
apply transitively to libraries called by the workflow. This is not a security
sandbox or automatic detection of every nondeterministic operation.

## Entry points

API sketch; method declarations omit implementation bodies:

```go
package bubble

type Options struct {
    Protocol  string // empty selects the prototype's sole supported protocol
    StartTime time.Time
    Seed      uint64
    RandomSource rand.Source // math/rand/v2.Source; nil selects PCG(Seed, 0)
}

// Identifies the experimental scheduling contract, not just the Go release.
const Protocol = "cooperative-v1"

// Target convenience API; deferred until it can reclaim failed/blocked runs.
func Run(opts Options, f func()) error

// New creates a paused bubble; it does not execute f yet.
func New(opts Options, f func()) (*Bubble, error)

type Activation struct {
    Now     time.Time
    Deliver func(*Delivery)
}

type State struct {
    Status         Status
    RootDone       bool
    LiveGoroutines int // user goroutines, excluding control/runtime goroutines
    TimerChanges   []TimerChange // ordered changes produced during this Step
}

type Status uint8

const (
    Quiescent Status = iota
    Completed
    Faulted
)

// Step applies an activation and runs until quiescent or all goroutines exit.
func (b *Bubble) Step(a Activation) (State, error)

// Close releases a finished or never-started bubble; rejects other live ones.
func (b *Bubble) Close() error

// StackTrace reports goroutines and block reasons at a quiescent boundary.
func (b *Bubble) StackTrace() string

type TimerID uint64
type TimerChangeKind uint8

const (
    TimerArmed TimerChangeKind = iota
    TimerDisarmed
)

type TimerChange struct {
    Kind       TimerChangeKind
    ID         TimerID
    Generation uint64
    Deadline   time.Time // meaningful for TimerArmed
}

// Delivery is valid only during its owning activation callback.
// FireTimer admits a host-authorized firing, not a new reading of the clock.
func (d *Delivery) FireTimer(id TimerID, generation uint64) bool
```

`Bubble` is opaque and has one host controller. Concurrent or reentrant Step,
entry from another bubble, and use after Close are errors. A zero StartTime
defaults to 2000-01-01 00:00:00 UTC. A zero activation Now retains the current
clock. If supplied on the first activation, Now must equal the effective
StartTime as an instant; subsequent supplied times must not go backward.
Normalize times to UTC without monotonic data. The prototype accepts positive,
representable signed Unix nanoseconds only: 1970-01-01 00:00:00.000000001 UTC
through 2262-04-11 23:47:16.854775807 UTC. Validate range before converting to
nanoseconds; saturate duration-added deadlines using Go's existing timer rules.
This explicit initial limitation avoids silently overflowing time.Time values.
Seed zero is a real,
reproducible seed, not a request for random initialization. The host records
Protocol and seed with its replay inputs, never chooses them from its clock.
Reject an unsupported protocol before executing user code.

Application math randomness is separately configurable through RandomSource.
Package-level math/rand and math/rand/v2 calls share that source and preserve its
state across activations; the default is a fresh PCG(Seed, 0). Its draw stream
does not consume scheduler choices. Replay requires a fresh source in the same
initial state. Explicit Rand objects retain their supplied sources, and legacy
package-level Seed is a no-op inside bubbles. The implementation synchronizes
source access and legacy byte buffering; sources must remain exclusive to their
bubble and must not block, yield, spawn, exit, or recursively call global random
APIs. crypto/rand is not redirected.

Only zero live user goroutines means Completed. RootDone does not stop clock
updates or execution of surviving children. Close is idempotent after success;
a rejected Close preserves the handle and execution state. StackTrace requires
a paused, completed, or faulted bubble and may not race Step.
Completed and Faulted are terminal: another Step is rejected before Deliver.
Delivery can enqueue work only in a never-stepped or Quiescent bubble; it cannot
resurrect Completed. A closed bubble releases its captured root closure.

The target `Run` entry point creates a bubble, steps it, requires all descendants
to exit, and closes it. A quiescent but unfinished self-contained execution is
an error. An error-only API would lose the handle to still-live goroutines, so
**do not implement or expose Run until safe disposal is specified**. The first
experiment uses New/Step so the controller retains the handle even after errors;
negative cases run in subprocesses. Run remains the intended eventual user
experience requested for self-contained code, not a claim that lifecycle is solved.

Preflight validation of lifecycle, protocol, and clock happens before mutation;
these validation errors leave the bubble usable. Delivery-time violations cannot
all be checked in advance: an unknown timer ID or full-mailbox send may occur
after earlier actions. Such a violation or recovered control-callback panic
marks the bubble Faulted and preserves its handle for diagnostics/disposal.
There is no rollback and no further Step. Partial timer observations are retained
as fault diagnostics, not published as a successful activation's output. No error
may silently release running goroutines back to the ordinary scheduler.

Errors cover controller misuse and unsupported execution detected by the
prototype. They do not imply that every runtime fatal error or arbitrary user
panic can be converted into an error. Panic propagation and containment must be
specified before the runtime experiment leaves a subprocess test harness. In
the earliest spike, an unrecovered panic in an ordinary child goroutine retains
Go's process-fatal behavior. Only panics at explicitly wrapped control callbacks
can become Faulted without a separate child-panic containment mechanism.

Ordinary user code retains its native shape:

```go
// Intended convenience API, after the disposal gate is resolved.
err := bubble.Run(bubble.Options{Seed: 7, StartTime: start}, func() {
    a, b := make(chan int), make(chan int)
    go func() { a <- 10 }()
    go func() { b <- 20 }()
    total := 0
    for i := 0; i < 2; i++ {
        select {
        case n := <-a:
            total += n
        case n := <-b:
            total += n
        }
    }
    if total != 30 {
        panic("unexpected total")
    }
})
```

## Host activation boundary

`Deliver` is a trusted host hook, executed **inside** the bubble with exclusive
logical control before suspended application goroutines resume. The hook must
return without blocking, performing I/O, or running user handlers inline. It may
fill owned mailboxes, close owned completion channels, and enqueue handler
goroutines. Those goroutines cannot run until the complete delivery batch ends.
An attempted blocking send/receive, explicit yield, or direct coroutine transfer
from the delivery hook is a contract violation, not ordinary quiescence. Detect
it before handing a logical turn to a peer. A callback can still loop forever;
the initial experiment uses subprocess watchdogs for that case.

The host assembles inputs in its recorded order. Closures themselves are not
serialized: on replay the host rebuilds delivery actions from its input log. Input
payloads must be immutable or copied, and the host must not retain mutable access
to bubble-owned state while Step runs. The runtime cannot prove arbitrary closure
or pointer purity; the host driver is part of the trusted determinism boundary.

```go
var result chan int // host retains the handle, but never operates on it directly
b, err := bubble.New(opts, func() {
    result = make(chan int, 1)
    n := <-result
    consume(n)
})
// Check err in real code.
state, err := b.Step(bubble.Activation{Now: opts.StartTime})
// state: root suspended, one live user goroutine.
state, err = b.Step(bubble.Activation{
    Now: recordedTaskTime,
    Deliver: func(*bubble.Delivery) { result <- recordedResult },
})
// The send runs inside the bubble; it does not cross an ordinary channel boundary.
```

Quiescence means no eligible user goroutine can progress internally. A root
blocked on a mailbox can remain quiescent between workflow tasks indefinitely.
The runtime cannot infer whether a future signal or activity result will arrive;
the host decides whether the suspension is valid. Never treat all quiescence as
deadlock, and never advance to an arbitrary future timer to escape it.

## Scheduling rules to implement and test

1. Assign bubble-local goroutine IDs in creation order. Start with FIFO runnable
   ordering; a child enters the tail and does not immediately preempt its parent.
2. One user goroutine owns the logical turn. It retains it until a supported
   blocking operation, explicit `runtime.Gosched`, or exit. A successful
   nonblocking channel operation does not itself yield. Awakened goroutines join
   the queue in a specified order. Channel close wakes existing waiters in their
   specified registration order, with duplicate select registrations deduplicated.
3. Physical preemption may pause the owner for GC or host fairness, but must not
   hand its logical turn to a peer. CPU loops can starve their bubble; a host
   watchdog reports a task-level diagnostic, never a replay-visible decision.
4. Select uses an unbiased seeded polling permutation over eligible cases.
   Specify the PRNG algorithm, candidate enumeration, draw consumption, and
   rejection sampling as part of Protocol. Do not use per-M runtime randomness,
   addresses, global goroutine IDs, or host timing. This stream is independent
   of application random numbers and any later timer-choice stream.
5. Audit all scheduling entries, including optimized selects, `reflect.Select`,
   readiness fast paths, canceled parking, coroutine/iterator switches, and
   race-enabled execution. Exclude an unsupported path explicitly rather than
   accidentally letting it use ordinary queues.
6. Keep the default Go scheduler and synctest policy unchanged outside this mode.
   Multiple independent bubbles can execute concurrently on different Ps.

These are proposed rules, not inherited guarantees from Go's current scheduler.
The exact rules must survive the runtime feasibility experiment before API freeze.

## Ownership and memory contract

For the initial supported synchronization set, every nonnil channel used by a
bubbled goroutine must belong to that bubble. Enforce both directions, including
ready buffered sends/receives, closed channels, nonblocking probes, and all select
cases. A nil channel retains ordinary Go semantics. The host uses Step rather
than directly sending on a bubble channel. Cross-bubble communication is rejected.

This is stronger than synctest's existing checks. It prevents a workflow wait
from depending on an unrecorded outside goroutine. It does not prevent a captured
pointer from observing outside memory. Require immutable inputs and race-free,
owned mutable state; do not claim total memory isolation.

Step return must establish a synchronization boundary for observing completed
bubble work; activation must establish one for delivered inputs. Normal channel
happens-before edges remain. A serialized scheduler must not silently declare
all otherwise racy user accesses safe. Race-detector semantics are a design gate.

## Go-owned clock and timer boundary

The runtime exposes timer intent and accepts authorized delivery. It knows
nothing about Temporal commands, event types, activity IDs, or storage.

- Creating or positively resetting a supported timer produces TimerArmed with a
  stable bubble-local ID, a new generation, and a deadline. Sleep uses the same
  machinery internally. Stop produces TimerDisarmed for an active generation.
  Preserve the operation order, including create-and-stop within a single Step.
- `State.TimerChanges` is an immutable snapshot of changes since the preceding
  Step. Reading it never requires the host to access an owned channel or timer.
- Moving `Activation.Now` updates clock reads but does not authorize firing.
  `Delivery.FireTimer` enqueues the matching timer's effect while user execution
  remains paused for the rest of the delivery batch.
- A valid armed generation cannot fire before its deadline. A stale, already
  fired, or canceled generation returns false without affecting the new timer.
  An unknown ID, early authorization, or invalid Delivery use is a contract
  violation caught by the control boundary and faults the activation; prior
  delivery actions are not rolled back. A false return is reserved for known
  stale, canceled, or already-authorized generations.
- Each generation accepts host authorization once. For channel timers,
  authorization makes the timer eligible; it is distinct from the program
  actually receiving from timer.C. Preserve Go's channel-timer Stop/Reset
  guarantees, including true for a still-running, unreceived timer and suppression
  of stale receives after either operation. Track authorization, pending send,
  and consumed state separately. Repeated equal-deadline authorizations follow
  the order explicitly supplied by the host.
- A late timer.C receive carries the original scheduled deadline, following
  `time.sendTime`'s adjustment for delayed delivery, rather than the later
  activation time. Timer reset invalidates that prior value.
- Nonpositive Sleep returns immediately. Nonpositive NewTimer/Reset needs a
  defined local-ready path without a durable-host round trip; specify its exact
  queue order and ordinary Go compatibility in the timer milestone. No wall-clock
  dependency or inline execution of an AfterFunc callback is allowed.

This permits a host to delay a timer until a recorded input arrives, even when
the logical clock has moved beyond the deadline. A next-deadline value alone
cannot express cancellation, reset generations, and separately authorized fires.
Arbitrary host batching is not promised replay-equivalent; activation boundaries
and ordered delivery are part of the input contract.

Reset emits TimerDisarmed for a still-armed prior registration, then TimerArmed
for the new generation; the ID identifies the timer object. The operation log
must not acquire nondeterministic cancellation entries from GC. For the MVP,
retain registrations until fired, explicitly stopped, or disposed, and measure
the memory tradeoff; do not claim ordinary GC reclamation behavior unchanged.
Never execute host code under runtime timer locks.

Clock virtualization also needs a location policy: current `time.Now` constructs
results with the process-wide `time.Local` (`go/src/time/time.go:1347`). The initial
test harness pins that environment to UTC. Decide whether a production API
requires a fixed environment or overrides locations inside the bubble before
claiming replay across differently configured hosts. Normalizing activation
timestamps alone does not solve this.

## Standard context and lifecycle follow-up

**Context and sync:** audit context child ordering, shared closed channels,
AfterFunc, and deadline timers. Audit mutex ownership, contention/starvation,
Cond lockers, and WaitGroup storage limitations. Support only the combinations
proved replay-safe. Do not make map iteration globally deterministic merely to
work around one library's private iteration order.

**Disposal:** root return, all descendants exited, workflow completion, and cache
eviction are different events. Temporal can complete when its root returns and
must also discard suspended workflows. Its existing Close uses coroutine exit
machinery; ordinary blocked native goroutines do not expose that operation.
Define whether defers run, how blocking defers behave, how panics are attributed,
and how command emission is disabled during disposal. Cooperative cancellation
alone cannot reclaim arbitrary nil-channel waits or infinite loops.

Initially Close accepts only zero live user goroutines. Process isolation makes
the narrow Go experiment reclaimable; it is not the eventual execution model.
General disposal is a Go API design gate before exposing Run or claiming that
the runtime surface is sufficient for a future Temporal SDK. No SDK teardown
implementation is designed here.

## Alternatives and decisions

| Approach | Benefit | Reason for the recommendation |
| --- | --- | --- |
| Extend existing bubble internals with an opt-in deterministic policy | Native syntax; reuses inheritance and time integration | Recommended experiment; largest unknowns are scheduler coverage, lifecycle, and maintenance cost. |
| Continue SDK cooperative wrappers | Works without a Go fork; existing semantics | Keep as compatibility baseline; does not achieve native go/chan/select. |
| Make synctest itself deterministic | Existing public entry point | Reject: testing policy, automatic time, testing.T dependency, and randomized behavior do not match durable execution. |
| Expose arbitrary pluggable scheduler hooks | Broader customization | Defer: expands runtime callback safety and compatibility commitments before proving one policy. |
| Record every scheduling decision | Can reproduce a broader set of runs | Defer: extra durable data and protocol complexity; existing Temporal histories do not contain this trace. |

The eventual Go proposal should request the smallest general-purpose mechanism
justified by the prototype. It need not expose the entire experimental API above.
