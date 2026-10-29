# Async Pool adaptation

Updated 2026-10-26. Native Rust implementation informed by `socketry/async-pool` revision [`ce077a745b4436e9f8474fd4554ab7d405eebc37`](https://github.com/socketry/async-pool/tree/ce077a745b4436e9f8474fd4554ab7d405eebc37). Reviewed `lib/async/pool/controller.rb`, `resource.rb`, and controller/multiplex/failure/retire-race tests. The upstream MIT license and contributor notices are preserved in [license.md](../license.md). This is a semantic adaptation, not a direct translation of Ruby task machinery. The upstream Ruby tests were inspected, not executed.

## Separate contracts

`SingleplexPool` transfers exclusive resource ownership into a non-cloneable lease. Its slot stores an optional idle resource and a draining flag. It does not require `Sync` or model stream reservations. Explicit release certifies clean reuse; unfinished drop retires the registration. An external driver can invalidate an active registration without taking the resource away from its owner.

`MultiplexPool` shares an immutable resource handle and tracks unopened reservations separately from active uses. The protocol owner activates at its opening boundary and revalidates current admission. Limit reductions and retirement can reject an unopened reservation without declaring an operation processed. Uses transfer into exchange state and remain charged through half closure or abandonment. Abandoned-use cleanup is retrieved by the resource driver and explicitly acknowledged. No pool callback opens protocol streams or acquires a mutex for the application resource.

A common `Pool` trait exposes acquisition, statistics and admission closure with distinct associated permit types. Private machinery shares FIFO waiters, construction accounting and retirement. This keeps common lifecycle behavior without treating every resource as a connection with an integer concurrency property.

Concurrent occupancy and consumable opening credit are distinct. The latter is decremented on activation and replenished only by an external update. This permits future QUIC integration to publish actual remaining opening credit. The pool does not interpret HTTP settings, GOAWAY, QUIC stream identifiers or peer acceptance. The initial API has per-pool bounds; a shared budget across typed pools remains future work.

## Gardener decision

The Ruby gardener is started when construction begins, invokes the optional policy and waits for pool notifications. Its cleanup closes the pool. It therefore supplies a scope/lifetime boundary even without a pruning policy. It is not intrinsically a periodic idle-expiry mechanism.

Base keeps its policy and lifetime responsibilities explicit:

- Admission closure and waiters are pool state.
- `prune(retain)` selects unused resources synchronously.
- Retirement transfers the resource into a cleanup queue.
- An owning operation consumes cleanup work, awaits resource-specific retirement and acknowledges completion.
- A client/server scope owns and joins that operation and all active exchanges.

No task starts implicitly on construction. Time-based eviction still needs a timer even when no acquire/release occurs; opportunistic pruning alone cannot provide it. Add that timer as an explicitly owned maintenance future when an idle/lifetime policy is implemented, using a supplied `Clock` domain. The protocol's connection reader remains a separate responsibility.

## Accounting and ownership

Construction is reserved before invoking a factory, with separate hard limits for total resources and concurrent constructors. `building + live + retiring` never exceeds the configured resource limit. A successful commit publishes a registration; the constructor then acquires a use through the same FIFO interface as other callers. A late commit after admission closure enters retirement. Factories remain outside the pool and retain their permits through construction cancellation and I/O cleanup.

Draining registrations reject new uses while retaining all existing accounting. Once their final use terminates, they move to retirement. Resource capacity remains charged while cleanup is queued or in progress. A dropped cleanup handle requeues the same obligation rather than asserting that I/O retired. Explicit completion destroys the resource before freeing capacity. Final destruction when all owners disappear cannot substitute for a supervised asynchronous shutdown.

No user factory, asynchronous wait or resource destructor runs under the pool-state mutex. Notifications occur after pool-state mutation. The retirement queue stores raw resources and IDs, not handles retaining the core, avoiding an internal ownership cycle. Registration control handles are weak; late notifications cannot affect replacement registrations because IDs are never recycled. Resource handles supplied by applications can still create cycles of their own and must follow ordinary ownership rules.

Acquire waiters are bounded and FIFO by admission opportunity. Notification is registered before condition rechecks; cancelled or dropped waiters withdraw their position and wake successors. Resource selection is stable registration order, packing existing capacity before construction. This differs deliberately from Ruby's available-set reinsertion ordering.

## HTTP integration and remaining work

The HTTP client is intended to use `MultiplexPool` for negotiated connections, including HTTP/1 with capacity one. Its active use belongs to exchange completion through request transmission and response consumption. Returning response metadata or even response EOF does not imply that an active upload finished. A pooled admission handle can remain separate from its scheduler-bound connection driver. `SingleplexPool` remains available for general exclusively leased resources.

For HTTP/2, reservation activation occurs at the connection driver's stream-opening boundary. The active use belongs to exchange state, with cleanup on reset/failure or completion of both directions. The background frame reader supplies input queues independently. GOAWAY disables new reservations while accepted streams drain.

These interfaces are implemented and tested independently of HTTP; the high-level client has not been implemented or integrated. Client-owned construction, driver registration, protocol validation, health probes and retry/timeout policy remain consumer work. A construction reservation can cover dialing and ALPN selection before committing the negotiated connection and its admission to the same multiplex pool. Shared budgets across distinct pools remain separate generic future work.

## Validation

Tests cover exclusive ownership of resources that are not `Sync`, borrowed resources, identity, construction failures and bounds, FIFO/cancelled/dropped waiters, shutdown racing commit, explicit pruning, retirement/release ordering, deferred cleanup and requeued acknowledgments. Multiplex cases cover concurrent uses, admission reductions at activation, availability updates without release, consumable opening credit, active abandonment, cleanup retry and draining. Independent-thread stress exercises hard limits and exclusive/concurrent use accounting.

A real TCP test performs two exchanges over one pooled socket and verifies that replacement construction remains blocked through resource closure until retirement acknowledgment. It does not claim HTTP client integration, alternative runtime conformance or performance results.

Validation on 2026-10-26: all 28 tests (25 pool behavior tests, one TCP test and two doctests) pass on the current toolchain and Rust 1.85.1. `cargo clippy --locked --all-targets -- -D warnings`, rustdoc with warnings denied, and `cargo fmt --check` pass. Local Markdown link targets across the pool and HTTP prototype packages were also checked.

## Admission and maintenance cost (2026-10-29)

The HTTP client now integrates the released multiplex contract, including capacity-one HTTP/1 connections. Its normal cycle must leave cleanup idle: reserving or completing a reusable use does not create retirement work. A separate retirement event wakes cleanup when a resource enters or re-enters its queue, shutdown starts, construction aborts, or retirement is acknowledged. Admission and abandoned-use observers retain the original shared event. Every retirement wait registers before checking both the queue and final closure; shutdown can still finish with no resources or after the last constructor is dropped.

An immediate acquisition with no queued waiters does not notify other observers: it only consumes capacity. Successful activation changes Reserved to Active in place and does not notify, since it creates neither capacity nor cleanup work. Rejection still removes the reservation and notifies; FIFO handoff after a queued acquisition also still notifies successors. Concurrent capacity and consumable opening credit keep their existing accounting. Rust's B-tree retains its empty root leaf in the capacity-one cycle, so this change saves operations rather than allocation calls.

Wake-count regressions verify that normal reservation, activation, release and capacity updates leave a retirement waiter asleep. They also verify actual wakes for queued cleanup, dropped cleanup requeue, final acknowledgment, empty shutdown and the last abandoned construction. Existing concurrent admission, cancellation, late commit and cleanup tests remain applicable. This changes private scheduling mechanics without changing pool interfaces or ownership obligations.

## Uncontended acquisition (2026-10-30)

An acquisition first checks cancellation and, under the state lock, takes an available resource or a construction reservation when no waiter is queued. Only an acquisition that cannot proceed registers a listener and joins the FIFO queue, so queued waiters still cannot be bypassed. Selection keeps stable registration order: the state records an identifier below which no slot is available. Commits, use and lease returns, rejected or dropped reservations and admission updates lower it to the affected identifier; a successful selection raises it to the chosen slot, and a failed search raises it past every registration. Draining, pruning and closure only reduce availability. Multiplex use stages are a list kept sorted by allocation order, which retains its storage across uses.

On Linux with the HTTP client benchmark at 32 concurrent HTTP/1 requests over TCP, user instructions per response fall from 19,479 to 18,907 and L1 data-cache misses from 586 to 562. Cycles and throughput move by about 1%, within run-to-run variation. The isolated capacity-one pool cycle falls from 2,555 to 2,106 instructions with the listener change alone. A synchronous `try_acquire` for established resources saved a further 245 instructions per response without reducing cycles or raising throughput, so it was not retained.
