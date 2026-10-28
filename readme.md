# Base Pool

A Rust adaptation of Socketry's `async-pool`, with separate singleplex and multiplex contracts. The initial API is experimental. Pools manage resource admission, construction reservations and retirement. They spawn no tasks and retain no scheduler instance of their own; resources may retain bound I/O capabilities. See the [release notes](releases.md).

| Contract | `SingleplexPool<R>` | `MultiplexPool<R>` |
| --- | --- | --- |
| Acquisition | Exclusive `SingleplexLease<R>` with mutable resource access. | Shared `MultiplexReservation<R>`, activated into one `MultiplexUse<R>`. |
| Resource bounds | `R: Send`, including resources that are not `Sync`. | `R: Send + Sync`; users receive shared access to a resource handle. |
| Completion | Consume the lease with `release` once clean and reusable. | The exchange owner consumes the active use with `complete` after protocol completion. |
| Abandonment | Unfinished lease drop retires the resource. | Unopened reservation drop returns admission. Active use drop reports abandonment and retains its charge until driver cleanup. |
| Availability changes | Return a clean lease or retire a registration. | Publish updated concurrent limits and optional opening credit through the registration handle. |

Both implement `Pool`, with distinct associated permit/build-permit types. Internal FIFO waiting, accounting, construction and retirement mechanisms are shared. The singleplex resource representation has no stream-use map or multiplex capacity counter.

`acquire_or_reserve` returns `Acquisition::Existing` or `Acquisition::Build`. Build permits reserve capacity **before** asynchronous construction. Register the progress owner, commit the resource, then acquire again to use it. The factory and its errors remain with the caller. A commit racing shutdown moves the resource into cleanup and returns `Error::Closed`.

```rust
use base_pool::{Acquisition, Options, SingleplexPool};
use base_scheduler::Cancellation;

futures_lite::future::block_on(async {
    let pool = SingleplexPool::new(Options { max_resources: 1, ..Options::default() })?;
    let cancellation = Cancellation::never();
    let mut resource = loop {
        match pool.acquire_or_reserve(&cancellation).await? {
            Acquisition::Existing(lease) => break lease,
            Acquisition::Build(build) => {
                // Construct the real resource and install its driver here.
                build.commit(String::from("hello"))?;
            }
        }
    };
    resource.push('!');
    assert_eq!(&*resource, "hello!");
    resource.release();

    pool.begin_close();
    while let Some(retired) = pool.next_retirement(&cancellation).await? {
        // Await resource-specific cleanup here, then acknowledge it.
        retired.complete();
    }
    pool.wait_closed(&cancellation).await?;
    Ok::<(), base_pool::Error>(())
})?;
# Ok::<(), base_pool::Error>(())
```

`Options` bounds building + live + retiring resources, simultaneous construction and queued waiters. Zero waiter capacity allows immediate acquisition but rejects waiting. Resource selection uses stable registration order and packs existing capacity before constructing. Waiters receive admission opportunities in FIFO order; constructing a resource is itself one opportunity and does not reserve its first use. Cancellation or dropping a pending acquisition removes its waiter without consuming capacity. IDs describe registrations, independent of resource equality, and are never recycled within the pool.

Multiplex admission separates concurrent-use capacity from consumable opening credit. `activate` revalidates a reservation against current limits and draining state; the protocol driver also validates actual protocol opening. Updating admission wakes waiters without requiring a release. Reducing a limit preserves active uses, and completing a use never replenishes consumable opening credit.

```rust
use base_pool::{Acquisition, Admission, MultiplexPool, Options};
use base_scheduler::Cancellation;

futures_lite::future::block_on(async {
    let pool = MultiplexPool::new(Options::default())?;
    let cancellation = Cancellation::never();
    let Acquisition::Build(build) = pool.acquire_or_reserve(&cancellation).await?
        else { unreachable!() };
    let registration = build.commit("connection handle", Admission::concurrent(2))?;
    let Acquisition::Existing(reservation) = pool.acquire_or_reserve(&cancellation).await?
        else { unreachable!() };
    let active = reservation.activate()?;
    assert_eq!(*active, "connection handle");

    // Transfer `active` to the protocol exchange owner. On completion:
    active.complete();
    registration.retire();
    pool.try_retirement().unwrap().complete();
    pool.begin_close();
    pool.wait_closed(&cancellation).await?;
    Ok::<(), base_pool::Error>(())
})?;
# Ok::<(), base_pool::Error>(())
```

A resource driver receives dropped active uses through `MultiplexResource::next_abandoned` or `try_abandoned`. It performs stream reset/termination and then completes the `MultiplexAbandonedUse`. Dropping that cleanup handle reports the abandoned use again. An application body close should notify its exchange owner; it must not release an active use while the other direction or reset is still running.

Maintenance is explicit. `prune(retain)` retires unused registrations; a registration's `retire` stops new uses and waits for existing owners. `try_retirement` and `next_retirement` transfer resources to an owned cleanup operation. `Retirement::complete` acknowledges finished cleanup and then frees resource capacity. Dropping unfinished retirement requeues it, preserving its charge. Cleanup must safely resume or tolerate retry after interruption.

`begin_close` stops admission, wakes waiters and drains registrations. `wait_closed` waits for construction, active work and resource retirement acknowledgments. It does not run cleanup or force cancellation of resource users. A caller supervises maintenance alongside active work; sequentially waiting for closure before draining cleanup would not make progress. Construction owners retain build permits until abandoned construction I/O is safely retired. Dropping all owners provides ordinary destruction only, not an asynchronous cleanup guarantee.

There is no built-in gardener. Retain/prune policy and cleanup belong in an explicitly driven operation, such as the client driver. A timer-driven maintenance future can later implement idle timeout or maximum lifetime using a supplied `Clock`. Without such an operation, unused resources remain until explicit pruning or shutdown. Shared budgets across typed pools, automatic factories, retry/health policies and timed maintenance remain follow-up work. No HTTP-specific behavior or concrete runtime is a production dependency.

The [port notes](design/port.md) record source provenance, the gardener decision, lifecycle changes and validation. Run `cargo test --locked` and `cargo clippy --locked --all-targets -- -D warnings`. Rust 1.87 is the minimum for version 0.2, which uses `base-scheduler 0.2`.
