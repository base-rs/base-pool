// Released under the MIT License.
// Copyright, 2019-2026, by Samuel Williams.

use base_pool::{
    Acquisition, Admission, Error, MultiplexBuildPermit, MultiplexPool, MultiplexReservation,
    Options, Pool, SingleplexBuildPermit, SingleplexLease, SingleplexPool,
};
use base_scheduler::Cancellation;
use futures_lite::future::{block_on, poll_once, yield_now, zip};
use std::{
    cell::Cell,
    pin::pin,
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Wake, Waker},
};

#[derive(Default)]
struct Wakes(AtomicUsize);
impl Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn retirement_waiter_ignores_normal_admission_but_wakes_for_cleanup_and_requeue() {
    let pool = MultiplexPool::new(options(1)).unwrap();
    let resource = multi_build(&pool)
        .commit(1, Admission::concurrent(1))
        .unwrap();
    let never = Cancellation::never();
    let wakes = Arc::new(Wakes::default());
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    let mut retirement = pin!(pool.next_retirement(&never));
    assert!(retirement.as_mut().poll(&mut cx).is_pending());
    for _ in 0..3 {
        drop(reserve(&pool));
        reserve(&pool).activate().unwrap().complete();
        resource.update(Admission::concurrent(1));
    }
    assert_eq!(wakes.0.load(Ordering::SeqCst), 0);
    resource.retire();
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    let std::task::Poll::Ready(Ok(Some(retired))) = retirement.as_mut().poll(&mut cx) else {
        panic!("retirement was not delivered");
    };
    let mut retirement = pin!(pool.next_retirement(&never));
    assert!(retirement.as_mut().poll(&mut cx).is_pending());
    wakes.0.store(0, Ordering::SeqCst);
    drop(retired);
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    let std::task::Poll::Ready(Ok(Some(retired))) = retirement.as_mut().poll(&mut cx) else {
        panic!("unfinished cleanup was not requeued");
    };
    pool.begin_close();
    let mut retirement = pin!(pool.next_retirement(&never));
    assert!(retirement.as_mut().poll(&mut cx).is_pending());
    wakes.0.store(0, Ordering::SeqCst);
    retired.complete();
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    assert!(matches!(
        retirement.as_mut().poll(&mut cx),
        std::task::Poll::Ready(Ok(None))
    ));
    assert_eq!(pool.stats().total(), 0);
}

#[test]
fn retirement_waiter_wakes_for_empty_shutdown_and_last_aborted_constructor() {
    for constructing in [false, true] {
        let pool = SingleplexPool::<usize>::new(options(1)).unwrap();
        let build = constructing.then(|| single_build(&pool));
        let never = Cancellation::never();
        let wakes = Arc::new(Wakes::default());
        let waker = Waker::from(wakes.clone());
        let mut cx = Context::from_waker(&waker);
        let mut retirement = pin!(pool.next_retirement(&never));
        assert!(retirement.as_mut().poll(&mut cx).is_pending());
        pool.begin_close();
        assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
        if constructing {
            assert!(retirement.as_mut().poll(&mut cx).is_pending());
            wakes.0.store(0, Ordering::SeqCst);
            drop(build);
            assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
        }
        assert!(matches!(
            retirement.as_mut().poll(&mut cx),
            std::task::Poll::Ready(Ok(None))
        ));
    }
}

fn options(max: usize) -> Options {
    Options {
        max_resources: max,
        max_constructing: max,
        max_waiters: 64,
    }
}
fn single_build<R: Send>(pool: &SingleplexPool<R>) -> SingleplexBuildPermit<R> {
    match block_on(pool.acquire_or_reserve(&Cancellation::never())).unwrap() {
        Acquisition::Build(permit) => permit,
        Acquisition::Existing(_) => panic!("expected construction"),
    }
}
fn lease<R: Send>(pool: &SingleplexPool<R>) -> SingleplexLease<R> {
    match block_on(pool.acquire_or_reserve(&Cancellation::never())).unwrap() {
        Acquisition::Existing(lease) => lease,
        Acquisition::Build(_) => panic!("expected a reusable resource"),
    }
}
fn multi_build<R: Send + Sync>(pool: &MultiplexPool<R>) -> MultiplexBuildPermit<R> {
    match block_on(pool.acquire_or_reserve(&Cancellation::never())).unwrap() {
        Acquisition::Build(permit) => permit,
        Acquisition::Existing(_) => panic!("expected construction"),
    }
}
fn reserve<R: Send + Sync>(pool: &MultiplexPool<R>) -> MultiplexReservation<R> {
    match block_on(pool.acquire_or_reserve(&Cancellation::never())).unwrap() {
        Acquisition::Existing(permit) => permit,
        Acquisition::Build(_) => panic!("expected resource capacity"),
    }
}
fn close_single<R: Send>(pool: &SingleplexPool<R>) {
    pool.begin_close();
    while let Some(retired) = pool.try_retirement() {
        retired.complete();
    }
    block_on(pool.wait_closed(&Cancellation::never())).unwrap();
    assert_eq!(pool.stats().total(), 0);
}
fn close_multi<R: Send + Sync>(pool: &MultiplexPool<R>) {
    pool.begin_close();
    while let Some(retired) = pool.try_retirement() {
        retired.complete();
    }
    block_on(pool.wait_closed(&Cancellation::never())).unwrap();
    assert_eq!(pool.stats().total(), 0);
}

#[test]
fn singleplex_exclusively_moves_send_resources_without_requiring_sync() {
    let pool = SingleplexPool::new(options(1)).unwrap();
    let resource = single_build(&pool).commit(Cell::new(0)).unwrap();
    let lease1 = lease(&pool);
    assert_eq!(lease1.id(), resource.id());
    lease1.set(7);
    assert_eq!(pool.stats().uses, 1);
    let clone = pool.clone();
    let waiting = std::thread::spawn(move || {
        let resource = lease(&clone);
        assert_eq!(resource.get(), 7);
        resource.set(8);
        resource.release();
    });
    lease1.release();
    waiting.join().unwrap();
    let result = lease(&pool);
    assert_eq!(result.get(), 8);
    result.release();
    close_single(&pool);
}

// Selection that skips an available registration would wait for capacity;
// fail instead of blocking the test.
fn lease_now<R: Send>(pool: &SingleplexPool<R>) -> SingleplexLease<R> {
    match block_on(poll_once(pool.acquire_or_reserve(&Cancellation::never()))) {
        Some(Ok(Acquisition::Existing(lease))) => lease,
        _ => panic!("expected an immediately available resource"),
    }
}
fn reserve_now<R: Send + Sync>(pool: &MultiplexPool<R>) -> MultiplexReservation<R> {
    match block_on(poll_once(pool.acquire_or_reserve(&Cancellation::never()))) {
        Some(Ok(Acquisition::Existing(permit))) => permit,
        _ => panic!("expected immediately available capacity"),
    }
}

#[test]
fn out_of_order_releases_still_select_the_earliest_available_registration() {
    let pool = SingleplexPool::new(options(3)).unwrap();
    let builds: Vec<_> = (0..3).map(|_| single_build(&pool)).collect();
    let ids: Vec<_> = builds
        .into_iter()
        .zip(0..)
        .map(|(build, index)| build.commit(index).unwrap().id())
        .collect();
    let leases: Vec<_> = (0..3).map(|_| lease_now(&pool)).collect();
    assert_eq!(leases.iter().map(|l| l.id()).collect::<Vec<_>>(), ids);
    let mut leases = leases.into_iter();
    let (first, second, third) = (
        leases.next().unwrap(),
        leases.next().unwrap(),
        leases.next().unwrap(),
    );
    third.release();
    first.release();
    let first = lease_now(&pool);
    assert_eq!(first.id(), ids[0]);
    let third = lease_now(&pool);
    assert_eq!(third.id(), ids[2]);
    second.release();
    let second = lease_now(&pool);
    assert_eq!(second.id(), ids[1]);
    for lease in [first, second, third] {
        lease.release();
    }

    let pool = MultiplexPool::new(options(2)).unwrap();
    let (first, second) = (multi_build(&pool), multi_build(&pool));
    let first = first.commit(0, Admission::concurrent(1)).unwrap();
    let second = second.commit(1, Admission::concurrent(1)).unwrap();
    let busy = reserve_now(&pool).activate().unwrap();
    assert_eq!(busy.resource_id(), first.id());
    let other = reserve_now(&pool).activate().unwrap();
    assert_eq!(other.resource_id(), second.id());
    // Raising admission makes an earlier busy resource available again.
    assert!(first.update(Admission::concurrent(2)));
    let shared = reserve_now(&pool);
    assert_eq!(shared.resource_id(), first.id());
    drop(shared);
    for active in [busy, other] {
        active.complete();
    }
    close_multi(&pool);
}

#[test]
fn equal_resources_have_distinct_registrations_and_stale_controls_cannot_retire_replacements() {
    let pool = SingleplexPool::new(options(2)).unwrap();
    let first = single_build(&pool).commit(0).unwrap();
    let held = lease(&pool);
    let second = single_build(&pool).commit(0).unwrap();
    assert_ne!(first.id(), second.id());
    held.release();
    assert_eq!(pool.stats().live, 2);
    assert!(first.retire());
    assert!(!first.retire());
    pool.try_retirement().unwrap().complete();
    let held = lease(&pool);
    assert_eq!(held.id(), second.id());
    let replacement = single_build(&pool).commit(0).unwrap();
    assert_ne!(replacement.id(), first.id());
    assert!(!first.retire());
    held.release();
    close_single(&pool);
}

#[test]
fn unfinished_singleplex_drop_retires_and_retirement_stays_charged_until_acknowledged() {
    let pool = SingleplexPool::new(options(1)).unwrap();
    single_build(&pool).commit(1).unwrap();
    drop(lease(&pool));
    assert_eq!(pool.stats().live, 0);
    assert_eq!(pool.stats().retiring, 1);
    block_on(async {
        let never = Cancellation::never();
        let mut acquire = pin!(pool.acquire_or_reserve(&never));
        assert!(poll_once(acquire.as_mut()).await.is_none());
        let retired = pool.try_retirement().unwrap();
        let id = retired.id();
        drop(retired);
        assert!(poll_once(acquire.as_mut()).await.is_none());
        assert_eq!(pool.stats().retiring, 1);
        let retired = pool.try_retirement().unwrap();
        assert_eq!(retired.id(), id);
        retired.complete();
        assert!(matches!(acquire.await.unwrap(), Acquisition::Build(_)));
    });
    close_single(&pool);
}

#[test]
fn retirement_while_leased_disables_reuse_and_defers_resource_cleanup() {
    let pool = SingleplexPool::new(options(1)).unwrap();
    let resource = single_build(&pool).commit(3).unwrap();
    let held = lease(&pool);
    assert!(resource.retire());
    assert_eq!(pool.stats().draining, 1);
    assert!(pool.try_retirement().is_none());
    held.release();
    assert_eq!(*pool.try_retirement().unwrap(), 3); // Unfinished cleanup requeues.
    assert_eq!(pool.stats().retiring, 1);
    close_single(&pool);
}

#[test]
fn construction_permits_enforce_resource_and_parallel_construction_limits() {
    block_on(async {
        let pool = SingleplexPool::<usize>::new(Options {
            max_resources: 2,
            max_constructing: 1,
            max_waiters: 8,
        })
        .unwrap();
        let first = single_build(&pool);
        let cancel = Cancellation::never();
        let mut waiting = pin!(pool.acquire_or_reserve(&cancel));
        assert!(poll_once(waiting.as_mut()).await.is_none());
        assert_eq!(pool.stats().building, 1);
        drop(first); // Failed or cancelled construction releases the reservation.
        let Acquisition::Build(first) = waiting.await.unwrap() else {
            panic!()
        };
        first.commit(1).unwrap();
        let held = lease(&pool);
        let second = single_build(&pool);
        assert_eq!(pool.stats().total(), 2);
        let mut full = pin!(pool.acquire_or_reserve(&cancel));
        assert!(poll_once(full.as_mut()).await.is_none());
        drop(second);
        assert!(matches!(full.await.unwrap(), Acquisition::Build(_)));
        held.release();
        close_single(&pool);
    });
}

#[test]
fn fifo_waiters_cannot_be_bypassed_and_cancelled_or_dropped_waiters_leave_no_holes() {
    block_on(async {
        let pool = SingleplexPool::new(options(1)).unwrap();
        single_build(&pool).commit(4).unwrap();
        let held = lease(&pool);
        let cancel1 = Cancellation::new();
        let never = Cancellation::never();
        let mut first = pin!(pool.acquire_or_reserve(&cancel1));
        assert!(poll_once(first.as_mut()).await.is_none());
        let mut second = pin!(pool.acquire_or_reserve(&never));
        assert!(poll_once(second.as_mut()).await.is_none());
        assert_eq!(pool.stats().waiters, 2);
        cancel1.cancel();
        assert!(matches!(first.await, Err(Error::Cancelled)));
        held.release();
        let mut newcomer = pin!(pool.acquire_or_reserve(&never));
        assert!(poll_once(newcomer.as_mut()).await.is_none());
        let Acquisition::Existing(second) = second.await.unwrap() else {
            panic!()
        };
        assert_eq!(*second, 4);
        second.release();
        let Acquisition::Existing(last) = newcomer.await.unwrap() else {
            panic!()
        };
        {
            let mut dropped = pin!(pool.acquire_or_reserve(&never));
            assert!(poll_once(dropped.as_mut()).await.is_none());
        }
        assert_eq!(pool.stats().waiters, 0);
        last.release();
        close_single(&pool);
    });
}

#[test]
fn waiter_bounds_precancellation_and_unpolled_acquisitions_do_not_consume_capacity() {
    block_on(async {
        let pool = SingleplexPool::<usize>::new(Options {
            max_waiters: 0,
            ..options(1)
        })
        .unwrap();
        drop(pool.acquire_or_reserve(&Cancellation::never()));
        assert_eq!(pool.stats().total(), 0);
        let cancelled = Cancellation::new();
        cancelled.cancel();
        assert!(matches!(
            pool.acquire_or_reserve(&cancelled).await,
            Err(Error::Cancelled)
        ));
        let build = single_build(&pool);
        assert!(matches!(
            pool.acquire_or_reserve(&Cancellation::never()).await,
            Err(Error::WaitersFull)
        ));
        drop(build);
        close_single(&pool);
    });
}

#[test]
fn shutdown_wakes_waiters_and_late_construction_is_retired_without_publication() {
    block_on(async {
        let pool = SingleplexPool::<usize>::new(options(1)).unwrap();
        let build = single_build(&pool);
        let never = Cancellation::never();
        let mut waiting = pin!(pool.acquire_or_reserve(&never));
        assert!(poll_once(waiting.as_mut()).await.is_none());
        pool.begin_close();
        assert!(matches!(waiting.await, Err(Error::Closed)));
        assert!(matches!(
            pool.acquire_or_reserve(&never).await,
            Err(Error::Closed)
        ));
        let mut closed = pin!(pool.wait_closed(&never));
        assert!(poll_once(closed.as_mut()).await.is_none());
        assert!(matches!(build.commit(5), Err(Error::Closed)));
        assert_eq!(pool.stats().building, 0);
        assert_eq!(pool.stats().retiring, 1);
        assert!(poll_once(closed.as_mut()).await.is_none());
        let retired = pool.next_retirement(&never).await.unwrap().unwrap();
        assert_eq!(*retired, 5);
        retired.complete();
        closed.await.unwrap();
        assert!(pool.next_retirement(&never).await.unwrap().is_none());
    });
}

#[test]
fn prune_retains_requested_live_count_and_never_retires_in_use_resources() {
    let pool = SingleplexPool::new(options(3)).unwrap();
    single_build(&pool).commit(1).unwrap();
    let a = lease(&pool);
    single_build(&pool).commit(2).unwrap();
    let b = lease(&pool);
    single_build(&pool).commit(3).unwrap();
    a.release();
    assert_eq!(pool.prune(2), 1);
    assert_eq!(pool.prune(2), 0);
    assert_eq!(pool.prune(0), 1);
    assert_eq!(pool.stats().live, 1);
    assert_eq!(pool.stats().uses, 1);
    b.release();
    close_single(&pool);
}

#[test]
fn multiplex_reservations_share_resource_and_dropping_unopened_uses_returns_capacity() {
    let pool = MultiplexPool::new(options(1)).unwrap();
    let resource = multi_build(&pool)
        .commit(Arc::new(AtomicUsize::new(0)), Admission::concurrent(2))
        .unwrap();
    let first = reserve(&pool);
    let second = reserve(&pool);
    assert_eq!(first.resource_id(), resource.id());
    assert_eq!(second.resource_id(), resource.id());
    assert_ne!(first.id(), second.id());
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(pool.stats().available, 0);
    first.fetch_add(1, Ordering::SeqCst);
    drop(first);
    assert_eq!(pool.stats().available, 1);
    let next = reserve(&pool);
    assert_eq!(next.load(Ordering::SeqCst), 1);
    assert_ne!(next.id(), second.id());
    drop((next, second));
    close_multi(&pool);
}

#[test]
fn multiplex_capacity_reductions_revalidate_reservations_without_terminating_active_uses() {
    let pool = MultiplexPool::new(options(1)).unwrap();
    let resource = multi_build(&pool)
        .commit(7, Admission::concurrent(3))
        .unwrap();
    let active = reserve(&pool).activate().unwrap();
    let reserved = reserve(&pool);
    resource.update(Admission::concurrent(1));
    assert!(matches!(reserved.activate(), Err(Error::AdmissionChanged)));
    assert_eq!(pool.stats().uses, 1);
    assert_eq!(*active, 7);
    resource.update(Admission::concurrent(0));
    active.complete();
    assert_eq!(pool.stats().available, 0);
    resource.update(Admission::concurrent(2));
    assert_eq!(pool.stats().available, 1);
    close_multi(&pool);
}

#[test]
fn multiplex_updates_wake_waiters_without_any_release() {
    block_on(async {
        let pool = MultiplexPool::new(options(1)).unwrap();
        let resource = multi_build(&pool)
            .commit(0, Admission::concurrent(0))
            .unwrap();
        let never = Cancellation::never();
        let mut waiting = pin!(pool.acquire_or_reserve(&never));
        assert!(poll_once(waiting.as_mut()).await.is_none());
        resource.update(Admission::concurrent(1));
        let Acquisition::Existing(reservation) = waiting.await.unwrap() else {
            panic!()
        };
        drop(reservation);
        close_multi(&pool);
    });
}

#[test]
fn multiplex_consumable_opening_credit_is_not_restored_by_use_completion() {
    let pool = MultiplexPool::new(options(1)).unwrap();
    let resource = multi_build(&pool)
        .commit(
            9,
            Admission {
                max_concurrent: 4,
                opening_credit: Some(1),
            },
        )
        .unwrap();
    let active = reserve(&pool).activate().unwrap();
    assert_eq!(pool.stats().available, 0);
    active.complete();
    assert_eq!(pool.stats().available, 0);
    resource.update(Admission {
        max_concurrent: 4,
        opening_credit: Some(1),
    });
    let pending = reserve(&pool);
    resource.update(Admission {
        max_concurrent: 4,
        opening_credit: Some(0),
    });
    assert!(matches!(pending.activate(), Err(Error::AdmissionChanged)));
    assert_eq!(pool.stats().uses, 0);
    close_multi(&pool);
}

#[test]
fn multiplex_abandonment_keeps_charge_until_driver_acknowledges_cleanup() {
    block_on(async {
        let pool = MultiplexPool::new(options(1)).unwrap();
        let resource = multi_build(&pool)
            .commit(8, Admission::concurrent(1))
            .unwrap();
        let active = reserve(&pool).activate().unwrap();
        let id = active.id();
        let never = Cancellation::never();
        let mut notice = pin!(resource.next_abandoned(&never));
        assert!(poll_once(notice.as_mut()).await.is_none());
        drop(active);
        assert_eq!(pool.stats().uses, 1);
        assert_eq!(pool.stats().available, 0);
        let abandoned = notice.await.unwrap().unwrap();
        assert_eq!(abandoned.id(), id);
        assert_eq!(*abandoned, 8);
        assert!(resource.try_abandoned().is_none());
        drop(abandoned); // A cancelled cleanup is retried with its charge intact.
        let abandoned = resource.try_abandoned().unwrap();
        assert_eq!(abandoned.id(), id);
        abandoned.complete();
        assert_eq!(pool.stats().uses, 0);
        assert_eq!(pool.stats().available, 1);
        close_multi(&pool);
        assert!(resource.next_abandoned(&never).await.unwrap().is_none());
    });
}

#[test]
fn multiplex_draining_stops_reservations_and_waits_for_every_use_before_retirement() {
    let pool = MultiplexPool::new(options(1)).unwrap();
    let resource = multi_build(&pool)
        .commit(4, Admission::concurrent(3))
        .unwrap();
    let a = reserve(&pool).activate().unwrap();
    let b = reserve(&pool).activate().unwrap();
    let c = reserve(&pool);
    assert!(resource.retire());
    assert!(!resource.retire());
    assert!(!resource.update(Admission::concurrent(10)));
    assert!(matches!(c.activate(), Err(Error::AdmissionChanged)));
    a.complete();
    assert_eq!(pool.stats().live, 1);
    assert!(pool.try_retirement().is_none());
    drop(b);
    assert!(pool.try_retirement().is_none());
    resource.try_abandoned().unwrap().complete();
    assert_eq!(pool.stats().live, 0);
    let retired = pool.try_retirement().unwrap();
    assert_eq!(**retired, 4);
    assert_eq!(pool.stats().retiring, 1);
    retired.complete();
    close_multi(&pool);
}

#[test]
fn multiplex_close_rejects_pending_openings_and_retains_active_use_ownership() {
    block_on(async {
        let pool = MultiplexPool::new(options(1)).unwrap();
        let resource = multi_build(&pool)
            .commit(0, Admission::concurrent(2))
            .unwrap();
        let active = reserve(&pool).activate().unwrap();
        let pending = reserve(&pool);
        pool.begin_close();
        assert!(matches!(pending.activate(), Err(Error::AdmissionChanged)));
        let never = Cancellation::never();
        let mut closed = pin!(pool.wait_closed(&never));
        assert!(poll_once(closed.as_mut()).await.is_none());
        assert!(!resource.update(Admission::concurrent(4)));
        active.complete();
        assert!(poll_once(closed.as_mut()).await.is_none());
        pool.try_retirement().unwrap().complete();
        closed.await.unwrap();
    });
}

#[test]
fn common_pool_trait_preserves_distinct_associated_permit_types() {
    async fn obtain<P: Pool>(pool: &P) -> P::BuildPermit {
        assert_eq!(pool.stats().total(), 0);
        match pool
            .acquire_or_reserve(&Cancellation::never())
            .await
            .unwrap()
        {
            Acquisition::Build(build) => build,
            Acquisition::Existing(_) => panic!(),
        }
    }
    fn close<P: Pool>(pool: &P) {
        pool.begin_close();
        assert!(pool.stats().closed);
    }
    let single = SingleplexPool::<String>::new(options(1)).unwrap();
    let multi = MultiplexPool::<String>::new(options(1)).unwrap();
    drop(block_on(obtain(&single)));
    drop(block_on(obtain(&multi)));
    close(&single);
    close(&multi);
    close_single(&single);
    close_multi(&multi);
}

#[test]
fn caller_can_supervise_maintenance_and_shutdown_without_pool_spawning() {
    block_on(async {
        let pool = SingleplexPool::new(options(1)).unwrap();
        single_build(&pool).commit(4).unwrap();
        let held = lease(&pool);
        pool.begin_close();
        let maintenance = async {
            let mut cleaned = 0;
            while let Some(retired) = pool.next_retirement(&Cancellation::never()).await.unwrap() {
                assert_eq!(*retired, 4);
                yield_now().await;
                assert_eq!(pool.stats().retiring, 1);
                retired.complete();
                cleaned += 1;
            }
            cleaned
        };
        let owner = async {
            yield_now().await;
            held.release();
            pool.wait_closed(&Cancellation::never()).await.unwrap();
        };
        assert_eq!(zip(maintenance, owner).await.0, 1);
    });
}

#[test]
fn arbitrary_resource_destructors_run_outside_pool_locks() {
    struct Reentrant {
        callback: Box<dyn Fn() + Send>,
    }
    impl Drop for Reentrant {
        fn drop(&mut self) {
            (self.callback)();
        }
    }
    let pool = SingleplexPool::<Reentrant>::new(options(1)).unwrap();
    let observed = Arc::new(AtomicUsize::new(0));
    let count = observed.clone();
    let clone = pool.clone();
    single_build(&pool)
        .commit(Reentrant {
            callback: Box::new(move || {
                assert_eq!(clone.stats().retiring, 1);
                count.fetch_add(1, Ordering::SeqCst);
            }),
        })
        .unwrap();
    drop(lease(&pool));
    pool.try_retirement().unwrap().complete();
    assert_eq!(observed.load(Ordering::SeqCst), 1);
    close_single(&pool);
}

#[test]
fn resources_can_borrow_application_storage() {
    let mut owned = String::from("borrowed");
    let pool = SingleplexPool::new(options(1)).unwrap();
    single_build(&pool).commit(&mut owned).unwrap();
    lease(&pool).push('!'); // Unfinished operation retires its borrowed resource.
    close_single(&pool);
    drop(pool);
    assert_eq!(owned, "borrowed!");
}

#[test]
fn concurrent_singleplex_operations_never_exceed_hard_limits_or_duplicate_ownership() {
    let pool = SingleplexPool::<Arc<AtomicUsize>>::new(options(3)).unwrap();
    let start = Arc::new(Barrier::new(9));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut threads = Vec::new();
    for _ in 0..8 {
        let pool = pool.clone();
        let start = start.clone();
        let seen = seen.clone();
        threads.push(std::thread::spawn(move || {
            start.wait();
            for _ in 0..100 {
                let lease = loop {
                    match block_on(pool.acquire_or_reserve(&Cancellation::never())).unwrap() {
                        Acquisition::Existing(lease) => break lease,
                        Acquisition::Build(build) => {
                            std::thread::yield_now();
                            build.commit(Arc::new(AtomicUsize::new(0))).unwrap();
                        }
                    }
                };
                assert_eq!(lease.fetch_add(1, Ordering::SeqCst), 0);
                assert!(pool.stats().total() <= 3);
                std::thread::yield_now();
                assert_eq!(lease.fetch_sub(1, Ordering::SeqCst), 1);
                seen.lock().unwrap().push(lease.id());
                lease.release();
            }
        }));
    }
    start.wait();
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(seen.lock().unwrap().len(), 800);
    assert!(pool.stats().live <= 3);
    assert_eq!(pool.stats().uses, 0);
    close_single(&pool);
}

#[test]
fn concurrent_multiplex_uses_respect_capacity_and_finish_exactly_once() {
    let pool = MultiplexPool::new(options(1)).unwrap();
    multi_build(&pool)
        .commit(AtomicUsize::new(0), Admission::concurrent(4))
        .unwrap();
    let start = Arc::new(Barrier::new(9));
    let mut threads = Vec::new();
    for _ in 0..8 {
        let pool = pool.clone();
        let start = start.clone();
        threads.push(std::thread::spawn(move || {
            start.wait();
            for _ in 0..100 {
                let active = reserve(&pool).activate().unwrap();
                assert!(active.fetch_add(1, Ordering::SeqCst) < 4);
                std::thread::yield_now();
                assert!(active.fetch_sub(1, Ordering::SeqCst) <= 4);
                active.complete();
            }
        }));
    }
    start.wait();
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(pool.stats().uses, 0);
    close_multi(&pool);
}

#[test]
fn reservation_order_after_capacity_reduction_is_decided_at_activation() {
    let pool = MultiplexPool::new(options(1)).unwrap();
    let resource = multi_build(&pool)
        .commit(0, Admission::concurrent(3))
        .unwrap();
    let first = reserve(&pool);
    let second = reserve(&pool);
    resource.update(Admission::concurrent(1));
    let active = second.activate().unwrap();
    assert!(matches!(first.activate(), Err(Error::AdmissionChanged)));
    assert_eq!(pool.stats().uses, 1);
    active.complete();
    close_multi(&pool);
}

#[test]
fn cancelling_maintenance_waits_preserves_cleanup_and_shutdown_obligations() {
    block_on(async {
        let pool = SingleplexPool::new(options(1)).unwrap();
        let resource = single_build(&pool).commit(0).unwrap();
        let cancel = Cancellation::new();
        {
            let mut cleanup = pin!(pool.next_retirement(&cancel));
            assert!(poll_once(cleanup.as_mut()).await.is_none());
            cancel.cancel();
            assert!(matches!(cleanup.await, Err(Error::Cancelled)));
        }
        resource.retire();
        assert!(matches!(
            pool.next_retirement(&cancel).await,
            Err(Error::Cancelled)
        ));
        assert_eq!(pool.stats().retiring, 1);
        pool.begin_close();
        assert!(matches!(
            pool.wait_closed(&cancel).await,
            Err(Error::Cancelled)
        ));
        close_single(&pool);
    });
}

#[test]
fn invalid_zero_resource_or_constructor_limits_are_rejected() {
    assert!(matches!(
        SingleplexPool::<usize>::new(Options {
            max_resources: 0,
            ..Options::default()
        }),
        Err(Error::InvalidOptions)
    ));
    assert!(matches!(
        MultiplexPool::<usize>::new(Options {
            max_constructing: 0,
            ..Options::default()
        }),
        Err(Error::InvalidOptions)
    ));
}

#[test]
fn errors_describe_each_failure() {
    let messages = [
        Error::Closed,
        Error::Cancelled,
        Error::WaitersFull,
        Error::AdmissionChanged,
        Error::InvalidOptions,
    ]
    .map(|error| error.to_string());
    assert_eq!(
        messages,
        [
            "pool admission is closed",
            "pool operation cancelled",
            "pool waiter limit reached",
            "resource admission changed before use activation",
            "resource and construction limits must be positive",
        ]
    );
}

#[test]
fn singleplex_handles_identify_registrations_and_leases_can_retire_explicitly() {
    let pool = SingleplexPool::new(options(1)).unwrap();
    let permit = single_build(&pool);
    let id = permit.id();
    let resource = permit.commit(1).unwrap().clone();
    assert_eq!(resource.id(), id);
    let held = lease(&pool);
    assert_eq!(held.id(), id);
    held.retire();
    assert!(!resource.retire());
    let mut retired = pool.try_retirement().unwrap();
    assert_eq!(retired.id(), id);
    *retired += 1;
    assert_eq!(*retired, 2);
    retired.complete();
    close_single(&pool);
}

#[test]
fn multiplex_handles_identify_registrations_and_report_closed_admission() {
    let pool = MultiplexPool::new(options(2)).unwrap();
    let permit = multi_build(&pool);
    let id = permit.id();
    let resource = permit.commit(1, Admission::concurrent(1)).unwrap().clone();
    assert_eq!(resource.id(), id);
    assert_eq!(pool.prune(0), 1);
    assert!(!resource.update(Admission::concurrent(2)));
    pool.try_retirement().unwrap().complete();
    let late = multi_build(&pool);
    pool.begin_close();
    assert!(matches!(
        late.commit(2, Admission::concurrent(1)),
        Err(Error::Closed)
    ));
    assert!(matches!(
        block_on(pool.acquire_or_reserve(&Cancellation::never())),
        Err(Error::Closed)
    ));
    assert_eq!(**pool.try_retirement().unwrap(), 2);
    close_multi(&pool);
}

#[test]
fn multiplex_abandonment_waits_report_cancellation_and_a_dropped_pool() {
    block_on(async {
        let pool = MultiplexPool::new(options(1)).unwrap();
        let resource = multi_build(&pool)
            .commit(0, Admission::concurrent(1))
            .unwrap();
        let cancel = Cancellation::new();
        {
            let mut notice = pin!(resource.next_abandoned(&cancel));
            assert!(poll_once(notice.as_mut()).await.is_none());
            cancel.cancel();
            assert!(matches!(
                poll_once(notice.as_mut()).await,
                Some(Err(Error::Cancelled))
            ));
        }
        assert!(matches!(
            poll_once(resource.next_abandoned(&cancel)).await,
            Some(Err(Error::Cancelled))
        ));
        let active = reserve(&pool).activate().unwrap();
        drop(active);
        let abandoned = resource.try_abandoned().unwrap();
        assert_eq!(abandoned.resource_id(), resource.id());
        abandoned.complete();
        drop(pool);
        assert!(!resource.update(Admission::concurrent(2)));
        assert!(resource.try_abandoned().is_none());
        assert!(matches!(
            poll_once(resource.next_abandoned(&Cancellation::never())).await,
            Some(Ok(None))
        ));
    });
}

#[test]
fn cancelling_a_close_wait_leaves_shutdown_pending() {
    block_on(async {
        let pool = SingleplexPool::new(options(1)).unwrap();
        single_build(&pool).commit(0).unwrap();
        pool.begin_close();
        let cancel = Cancellation::new();
        {
            let mut closed = pin!(pool.wait_closed(&cancel));
            assert!(poll_once(closed.as_mut()).await.is_none());
            cancel.cancel();
            assert!(matches!(
                poll_once(closed.as_mut()).await,
                Some(Err(Error::Cancelled))
            ));
        }
        assert_eq!(pool.stats().retiring, 1);
        close_single(&pool);
    });
}
