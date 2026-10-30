# Releases

## v0.3.0

- Use `base-scheduler 0.3`. Pool operations take its `Cancellation`, so callers must use the same scheduler version; pool behavior and admission accounting are unchanged.

- Wake retirement waiters only for cleanup work: a resource entering or re-entering the retirement queue, shutdown, aborted construction, or an acknowledged retirement. Normal reservation, activation, release and capacity updates no longer wake them, and successful multiplex activation no longer notifies other observers.

- Acquire an available resource without registering a listener when no waiter is queued, and select the earliest available registration without rescanning slots known to be unavailable. FIFO admission and selection order are unchanged. In the HTTP client benchmark at 32 concurrent requests, user instructions per response fall by about 3%.

## v0.2.0

- Use `base-scheduler 0.2` and require Rust 1.87. Pool acquisition and retirement share the corrected scheduler interface's cancellation type; pool behavior and admission accounting are unchanged.

## v0.1.0

Initial release of the runtime-independent resource pools. Requires Rust 1.85 or later. The API is experimental; HTTP client integration and pool performance measurements remain follow-up work.

- Provide distinct `SingleplexPool` exclusive leases and `MultiplexPool` shared-use reservations, with a common `Pool` interface.
- Bound total resources, concurrent construction and queued waiters. Reserve resource capacity before asynchronous construction and retain it through explicit retirement.
- Support FIFO admission, cancellation, capacity updates, reservation revalidation and consumable opening credit independently of concurrent-use limits.
- Retain abandoned active-use charges until the resource driver acknowledges cleanup. Requeue unfinished cleanup and retirement obligations.
- Expose explicit pruning, admission closure, retirement and shutdown waits. Spawn no tasks and retain no scheduler instance.
- Depend on the published `base-scheduler` interface and preserve the upstream MIT license and contributor notices from the `async-pool` adaptation.
