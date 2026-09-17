# Deterministic Go bubbles

An experimental Go runtime for running native Go concurrency inside an opt-in,
deterministic execution bubble. A future Temporal Go SDK supplies requirements;
designing or implementing that SDK is outside this project's scope.

- [Research: Temporal requirements, Go runtime support, and prior proposals](docs/research.md)
- [Proposed experimental API and its execution contract](docs/mvp-api.md)
- [Implementation milestones and verification gates](.omx/plans/deterministic-bubble-mvp.md)
- [Implemented API, verification, and limitations](docs/implementation.md)

The fork now provides `runtime/bubble`: native goroutines, owned channels,
deterministic select, independent clocks, quiescence, host-authorized timers, and
configurable deterministic application randomness.
Each bubble runs one logical user goroutine at a time; separate bubbles can run
in parallel. This is a research prototype, not an accepted Go proposal or an SDK.

Build and run the tests from this directory:

```sh
(cd go/src && ./make.bash)
GO111MODULE=off ./go/bin/go test runtime/bubble testing/synctest
GO111MODULE=off ./go/bin/go test runtime/bubble -run '^Example$' -v
```

Use this fork's `go/bin/go`, not the installed Go executable. GO111MODULE=off
keeps unrelated ancestor modules out of these standard-library test commands.

```go
b, err := bubble.New(bubble.Options{Seed: 7}, func() {
    time.Sleep(time.Minute)
    // Ordinary goroutines, channels, and select work inside the bubble.
})
if err != nil {
    panic(err)
}
state, err := b.Step(bubble.Activation{}) // returns with the sleep blocked
if err != nil {
    panic(err)
}
timer := state.TimerChanges[0]
state, err = b.Step(bubble.Activation{
    Now: timer.Deadline,
    Deliver: func(d *bubble.Delivery) {
        d.FireTimer(timer.ID, timer.Generation)
    },
})
if err != nil {
    panic(err)
}
// state.Status is bubble.Completed; Close now releases the bubble.
if err := b.Close(); err != nil {
    panic(err)
}
```

The tested [example](go/src/runtime/bubble/example_test.go) demonstrates two
independent clocks. `Close` currently refuses live goroutines; forced disposal
and general child-panic containment remain future runtime work.

Supply an application random source using `math/rand/v2`:

```go
// rand is imported from "math/rand/v2".
opts := bubble.Options{
    Seed:         7, // scheduler choices
    RandomSource: rand.NewPCG(42, 99),
}
```

Package-level calls to both `math/rand` and `math/rand/v2` use this source inside
the bubble, including descendants and delivery callbacks. Random state survives
Step boundaries and does not consume scheduler randomness. Omit RandomSource to
use a fresh `rand.NewPCG(Seed, 0)`. Give each bubble a fresh source; do not share
or mutate an owned source externally. Legacy `math/rand.Seed` is a no-op inside
bubbles. Explicit `rand.New` generators keep their sources; `crypto/rand` is
unchanged and remains outside deterministic-random support.

The [random-source example](go/src/runtime/bubble/example_random_test.go) shows
reproducible ordinary random calls with a supplied source.
