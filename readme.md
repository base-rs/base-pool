# `base-pool`

Explicit singleplex and multiplex resource pools for Base, adapted from Socketry's `async-pool`. Pools manage resource admission, construction reservations and retirement without spawning tasks or depending on a concrete runtime.

## Usage

Add the crate to your project:

```sh
cargo add base-pool
```

A `SingleplexPool` hands out exclusive leases. Acquisition either returns an existing resource or a build permit that reserves capacity before you construct one:

```rust
use base_pool::{Acquisition, Options, SingleplexPool};
use base_scheduler::Cancellation;

fn main() -> Result<(), base_pool::Error> {
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
        pool.wait_closed(&cancellation).await
    })
}
```

A `MultiplexPool` shares one resource handle between concurrent uses with separately tracked admission. See the [API documentation](https://docs.rs/base-pool) for the multiplex contract, lifecycle and maintenance, and the [port notes](design/port.md) for design decisions.

## Releasing

Prepare a release with `cargo bake cargo:release:patch` (or `minor`, `major`, or `bump --version X.Y.Z`), which bumps the version, commits it with a `Release: vVERSION` trailer, validates it and opens the release pull request. After review and merge, GitHub Actions publishes the release when the configured `crates-io` environment approves it, then creates or updates the matching GitHub Release from `releases.md`. Follow the shared [Releasing skill](https://github.com/base-rs/base-project/blob/main/context/releasing.md) for the standard process.

## Releases

<!-- bake-readme:releases:start -->

See [releases.md](releases.md) for the full release history.

### v0.5.0

- **Breaking:** Use `base-scheduler 0.5`. Pool operations take its `Cancellation`, so callers must use the same scheduler version; pool behavior and admission accounting are unchanged, and Rust 1.87 remains the minimum.

### v0.4.0

- **Breaking:** Use `base-scheduler 0.4`. Pool operations take its `Cancellation`, so callers must use the same scheduler version; pool behavior and admission accounting are unchanged.

### v0.3.0

- Use `base-scheduler 0.3`. Pool operations take its `Cancellation`, so callers must use the same scheduler version; pool behavior and admission accounting are unchanged.

- Wake retirement waiters only for cleanup work: a resource entering or re-entering the retirement queue, shutdown, aborted construction, or an acknowledged retirement. Normal reservation, activation, release and capacity updates no longer wake them, and successful multiplex activation no longer notifies other observers.

- Acquire an available resource without registering a listener when no waiter is queued, and select the earliest available registration without rescanning slots known to be unavailable. FIFO admission and selection order are unchanged. In the HTTP client benchmark at 32 concurrent requests, user instructions per response fall by about 3%.

<!-- bake-readme:releases:end -->

## See Also

- [base-scheduler](https://github.com/base-rs/base-scheduler) supplies the cancellation interface that pool operations use.

## Contributing

Please open an issue or pull request on [GitHub](https://github.com/base-rs/base-pool).
