# Implemented Go prototype

The implementation extends the existing runtime bubble machinery and adds the
fork-only `runtime/bubble` package. It does not change Go syntax, add dependencies,
or implement a Temporal SDK. The API is experimental and not proposal-ready.

## Supported behavior

- `New(Options, func())` creates a paused bubble; descendant `go` statements
  inherit it. `Step(Activation)` executes until quiescent or completed.
- Each bubble owns its clock, seeded select-choice stream, runnable FIFO, and
  logical execution permit. Independent bubbles can execute on different Ps.
- Options.RandomSource accepts a math/rand/v2.Source, including NewPCG and
  NewChaCha8. Package-level math/rand and math/rand/v2 calls share it within the
  bubble. A nil source defaults to a fresh PCG(Seed, 0), independently of the
  scheduler's stream. State persists across activations; fresh bubbles require
  fresh sources for replay. Legacy Read buffers are also bubble-local.
- The logical owner runs until a supported block, explicit Gosched, or exit.
  Physical preemption and GC assistance retain that logical owner.
- Native buffered/unbuffered channel operations and select, including
  reflect.Select, use bubble ownership checks. Non-nil outside channels are
  rejected even when an operation would not block. Nil channels keep their
  ordinary Go behavior.
- Channel close wakes waiting receivers in registration order, then waiting
  senders in registration order. New goroutines join the FIFO tail.
- `time.Now` uses the bubble's logical clock. Updating the clock does not itself
  fire positive-duration timers. Sleep and NewTimer/Stop/Reset report ordered
  registration changes; `Delivery.FireTimer` authorizes a matching generation.
- Nonpositive channel timers become locally ready. Native Stop/Reset drain stale
  authorized-but-unreceived values, preserving channel-timer guarantees.
- `State` distinguishes root return, all user goroutines exited, and quiescence.
  Quiescence does not mean deadlock. Surviving children continue after root exit.
- `StackTrace` reports the bubble's suspended goroutines. `Close` releases a
  completed or never-started bubble and rejects live goroutines without losing
  the handle. Controller calls reject nesting and concurrent use.

## Implementation map

| Files under `go/src/` | Responsibility |
| --- | --- |
| `runtime/bubble/bubble.go` | Public controller, validation, lifecycle, timer snapshots, delivery capability |
| `internal/runtime/bubble/bubble.go` | Small runtime bridge and shared representation |
| `runtime/bubble.go` | Logical owner/FIFO, activation handoff, seeded choices, diagnostics |
| `runtime/bubble_rand.go`, `math/rand/bubble.go`, `math/rand/rand.go`, `math/rand/v2/rand.go` | Dedicated application-random bridge, legacy state isolation, shared draws, and Read buffering |
| `runtime/synctest.go` | Shared bubble metadata/accounting with a separate deterministic policy |
| `runtime/proc.go` | Creation/readiness/parking/yield/exit scheduling hooks; owner checks |
| `runtime/chan.go`, `runtime/select.go` | Ownership, close ordering, select choice stream |
| `runtime/bubble_time.go`, `runtime/time.go` | Host timer registration/generations, authorized delivery, native timer semantics |
| `runtime/coro.go`, `runtime/sema.go`, `runtime/mgc.go`, `runtime/panic.go` | Explicit guards around unsupported control transfers and operations |
| `go/build/deps_test.go` | Standard-library dependency policy for the two new packages |
| `runtime/bubble/*_test.go` | Conformance tests, replay stress, negative subprocesses, runnable example |

Existing synctest tests retain their auto-advancing clock policy. Ordinary Go
goroutines continue to use normal scheduling. The deterministic mode does not
change process-wide GOMAXPROCS or disable preemption.

## Memory and failure boundaries

Race instrumentation uses separate addresses for activation publication and
quiescence collection. This establishes host/user handoff synchronization
without treating every cooperative context switch as user synchronization.
Programs must still be data-race-free; bubble membership does not isolate memory.

Application sources use a real mutex so package-level random calls remain safe
between sibling goroutines under the race detector. Legacy Read additionally
locks its buffered-byte state. Its lazily initialized generator is published to
peers with an initialization-only synchronization edge. General runtime entropy
for maps, allocation, and other housekeeping is untouched and never invokes the
application source.

Custom sources are exclusively owned by their bubble and must implement
deterministic, finite, nonblocking Uint64 calls. They must not yield, spawn
goroutines, call Goexit, or recursively call package-level randomness, including
legacy Read when it could consume buffered bytes. Guards reject these operations;
blocking/yielding violations may terminate the process. Source panics unwind
the source lock and recursion guard but do not roll back source state. Existing
bubble panic limitations continue to apply.

Delivery runs on the temporarily attached controller, before user goroutines
resume. It can close or fill owned channels and authorize timers. It must return
without blocking or yielding. A recovered delivery panic produces Faulted state
with retained diagnostics; it does not roll back earlier actions or run newly
queued user goroutines. Faulted execution cannot resume. Panic formatting does
not invoke arbitrary user Error/String methods outside the bubble.

Unrecovered child panics, prohibited channel operations, and some unsupported
blocking operations can terminate the process. There is no forced reclamation
of suspended native stacks. Use subprocess containment for negative experiments.
An error-only Run convenience function is deliberately absent because it could
otherwise lose the only handle to live goroutines after an error.

## Current restrictions

- No general Mutex/RWMutex contention, Cond wait, or context compatibility
  promise. An unowned WaitGroup wait is rejected; incidental success of other
  synchronization APIs is not a supported contract.
- No AfterFunc, Ticker, coroutine transfers (`iter.Pull`), LockOSThread, or
  explicit runtime.GC inside a bubble. Automatic GC and host-triggered GC are
  allowed. These restrictions prevent unsupported paths from silently escaping
  the logical schedule.
- Syscall entry and network blocking are rejected where the runtime observes
  them. Raw syscalls, unsafe, arbitrary external memory, package globals,
  map-dependent ordering, cryptographic/external randomness, finalizers, and ambient process
  state remain outside the determinism guarantee. This is not a sandbox.
- Clock inputs are positive signed Unix nanoseconds; zero options use the
  documented defaults. Arming timers at the maximum clock value is rejected
  because saturated deadlines cannot distinguish immediate from positive waits.
- `time.Now` still uses Go's process-wide Local location. Pin host configuration
  for replay, or use explicit UTC conversion. Timezone independence is not
  implemented.
- Timer registrations remain retained until Close, including canceled/fired
  entries, to recognize stale host generations. Long-lived timer-heavy bubbles
  therefore have unbounded registry growth in this prototype.
- Reproducibility is for the same supported program/toolchain/policy and ordered
  input batches. No cross-release, cross-architecture, or legacy-history replay
  compatibility has been established.
- Diagnostic stacks still contain ordinary Go goroutine IDs and process-specific
  details. They are not deterministic application outputs and must not be fed
  back into replay-sensitive decisions.
- Explicit rand.New generators retain their own sources, and crypto/rand remains
  unchanged. Inside a bubble math/rand.Seed is always a no-op, irrespective of
  GODEBUG; outside behavior is unchanged. Sharing one custom source across bubbles
  or using it outside its owning bubble violates the source ownership contract.

## Verification

Both the unmodified baseline and the completed fork built with
`go/src/make.bash` on darwin/arm64, including the toolchain bootstrap checks. The new package
has normal and race-enabled tests, including FIFO/yield, native/reflect select,
owned channels, independent concurrent bubbles, repeated quiescence, clock
validation, timer generations and stale suppression, and misuse rejection.
The executable example shows two clocks evolving independently.

Final checks after the ordinary-path optimizations all passed: runtime, bubble,
synctest, time, sync, and reflect tests; race-enabled bubble and synctest tests;
the 1,000-process replay run; public-package vet; dependency-policy tests;
formatting and diff checks. The optional staticlockranking baseline issue is
described separately below.

Random-source regressions additionally compare mixed legacy/v2 draws with a
reference PCG, test inherited/concurrent sources and buffered Reads, verify state
across activations, exercise Seed/GODEBUG host isolation, and check panic recovery
and forbidden source operations. Application draws and scheduler choices are
tested for independence in both directions. Run the standard math/rand tests
from `go/src` in module mode so their expected modern GODEBUG defaults are used:

```sh
(cd go/src && GO111MODULE=on GOWORK=off ../bin/go test math/rand math/rand/v2)
GO111MODULE=off ./go/bin/go test -race runtime/bubble -run Random
```

The random-source extension passed the rebuilt toolchain, full runtime/bubble/
synctest regressions, normal and race-enabled math/rand and math/rand/v2 tests,
vet, and dependency-policy checks. The updated 1,000-process replay fixture also
passed with mixed legacy/v2 application draws and unrelated host random calls.

Additional regressions keep a peer queued during a 16 MiB allocation section
while outside goroutines trigger GC, for both application execution and delivery.
A race-build-only subprocess intentionally performs unsynchronized peer writes
and requires a DATA RACE report. These check that logical serialization neither
breaks under GC nor hides actual user races.

Fresh-process replay compares observable output under GOMAXPROCS 1/2/8,
GOGC 20/100/off, and unrelated host CPU/allocation/explicit-GC load. The 1,000-process run
passed without trace divergence. This is evidence for the tested fixtures, not a
proof that arbitrary Go programs replay deterministically.

Useful commands from the workspace root:

```sh
GO111MODULE=off ./go/bin/go test runtime/bubble testing/synctest
GO111MODULE=off ./go/bin/go test -race runtime/bubble testing/synctest
GO111MODULE=off ./go/bin/go vet runtime/bubble
GO111MODULE=off BUBBLE_REPLAY_PROCESSES=1000 ./go/bin/go test runtime/bubble \
    -run '^TestReplayFreshProcesses$' -count=1 -timeout=180s
GO111MODULE=off GODEBUG=panicnil=0 ./go/bin/go test runtime time sync reflect
```

The explicit panicnil setting in the broader command avoids legacy default
behavior selected in module-disabled test builds; unrelated ancestor module
discovery otherwise breaks runtime's source-import alignment test here.

The optional staticlockranking experiment fails during pristine Go initialization
on this machine with a sysmon/allp/execR ordering violation. The identical failure
was reproduced from an archived unmodified HEAD. Bubble tests pass with
`GOEXPERIMENT=staticlockranking GODEBUG=asyncpreemptoff=1`; this workaround is only
for that diagnostic run. Normal and race tests run with normal preemption.

Full supported-platform validation, production resource bounds, general disposal,
panic containment, and a release compatibility policy remain required before
an upstream proposal or production claim. No whole-toolchain performance claim
is made by these functional tests.

## Initial ordinary-runtime cost measurements

Compared the pristine pinned Go revision with the initial prototype on this
darwin/arm64 machine, GOMAXPROCS=1, ten 100 ms samples per benchmark. Values below
are medians, not statistically established speedups or a whole-program result.
These measurements predate the configurable-random-source extension; overhead
of the new math-random API routing has not been separately benchmarked.

| Existing benchmark (outside bubbles) | Baseline ns/op | Prototype ns/op | Baseline → prototype B/op |
| --- | ---: | ---: | ---: |
| ChanNonblocking | 3.865 | 3.856 | 0 → 0 |
| SelectUncontended | 55.710 | 53.180 | 0 → 0 |
| Stop / channel timer | 166.650 | 164.000 | 248 → 264 |
| Stop / function timer | 291.000 | 290.200 | 264 → 280 |
| Reset / channel timer | 35.605 | 33.770 | 0 → 0 |
| Reset / function timer | 31.745 | 32.110 | 0 → 0 |

The first implementation added unconditional helper calls and inline timer
metadata. Guarding those calls on ordinary paths removed the measured
nonblocking-channel slowdown; moving timer metadata into an optional sidecar
halved the extra allocation cost. **Ordinary timer creation still costs 16 extra
bytes in these benchmarks** because the runtime timer has an additional pointer
and crosses an allocation size class. This needs attention before upstreaming.

The comparison used the existing runtime/time benchmarks with
`-run '^$' -bench '^(BenchmarkChanNonblocking|BenchmarkSelectUncontended|BenchmarkStop|BenchmarkReset)$'`
and `-benchtime=100ms -count=10 -benchmem`. Broader workloads and suspended-bubble
resource measurements remain future work; this small sample does not establish
negligible overhead for all Go programs.
