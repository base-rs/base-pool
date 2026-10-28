// Released under the MIT License.
// Copyright, 2019-2026, by Samuel Williams.

use crate::{Error, Options, ResourceId, Retirement, Stats, retirement::RetirementQueue};
use base_scheduler::Cancellation;
use event_listener::Event;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    future::{Future, poll_fn},
    pin::pin,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    task::Poll,
};

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

pub(crate) async fn cancellable<F: Future>(
    future: F,
    cancel: &Cancellation,
) -> Result<F::Output, Error> {
    cancel.check()?;
    let mut work = pin!(future);
    let mut cancelled = pin!(cancel.cancelled());
    poll_fn(|cx| {
        if cancelled.as_mut().poll(cx).is_ready() {
            Poll::Ready(Err(Error::Cancelled))
        } else {
            work.as_mut().poll(cx).map(Ok)
        }
    })
    .await
}

#[derive(Default)]
pub(crate) struct Counts {
    pub building: usize,
    pub live: usize,
    pub retiring: usize,
}
impl Counts {
    pub fn total(&self) -> usize {
        self.building + self.live + self.retiring
    }
}

pub(crate) struct Accounting {
    pub counts: Mutex<Counts>,
    pub changed: Event,
    // Cleanup only advances when retirement work arrives or shutdown can
    // finish. Admission/use changes must not wake an idle cleanup owner.
    pub retirement_changed: Event,
}
impl Accounting {
    pub fn notify(&self) {
        self.changed.notify(usize::MAX);
    }
}

pub(crate) trait Slot: Send {
    type Resource: Send;
    type Acquired: Send;
    fn available(&self) -> bool;
    fn acquire(&mut self) -> Self::Acquired;
    fn uses(&self) -> usize;
    fn draining(&self) -> bool;
    fn drain(&mut self);
    fn into_resource(self) -> Self::Resource;
}

pub(crate) struct State<S> {
    pub closed: bool,
    pub slots: BTreeMap<ResourceId, S>,
    building: BTreeSet<ResourceId>,
    waiters: VecDeque<u64>,
    next_resource: u64,
    next_waiter: u64,
}

pub(crate) struct Core<S: Slot> {
    pub state: Mutex<State<S>>,
    pub accounting: Arc<Accounting>,
    pub retirements: Arc<RetirementQueue<S::Resource>>,
    options: Options,
}

pub(crate) enum Acquired<T> {
    Existing(ResourceId, T),
    Build(ResourceId),
}

// A cancelled or dropped waiter gives its position back and wakes its successor.
struct Waiter<'a, S: Slot> {
    core: &'a Core<S>,
    ticket: Option<u64>,
}
impl<S: Slot> Drop for Waiter<'_, S> {
    fn drop(&mut self) {
        if let Some(ticket) = self.ticket {
            lock(&self.core.state).waiters.retain(|&id| id != ticket);
            self.core.accounting.notify();
        }
    }
}

impl<S: Slot> Core<S> {
    pub fn new(options: Options) -> Result<Arc<Self>, Error> {
        options.validate()?;
        let accounting = Arc::new(Accounting {
            counts: Mutex::new(Counts::default()),
            changed: Event::new(),
            retirement_changed: Event::new(),
        });
        let retirements = Arc::new(RetirementQueue::new(accounting.clone()));
        Ok(Arc::new(Self {
            state: Mutex::new(State {
                closed: false,
                slots: BTreeMap::new(),
                building: BTreeSet::new(),
                waiters: VecDeque::new(),
                next_resource: 0,
                next_waiter: 0,
            }),
            accounting,
            retirements,
            options,
        }))
    }
    pub fn lock(&self) -> MutexGuard<'_, State<S>> {
        lock(&self.state)
    }

    fn take(&self, state: &mut State<S>) -> Option<Acquired<S::Acquired>> {
        // Stable registration order packs available resources before building.
        if let Some((&id, slot)) = state.slots.iter_mut().find(|(_, slot)| slot.available()) {
            return Some(Acquired::Existing(id, slot.acquire()));
        }
        let mut counts = lock(&self.accounting.counts);
        if counts.total() >= self.options.max_resources
            || counts.building >= self.options.max_constructing
        {
            return None;
        }
        let id = ResourceId(state.next_resource);
        state.next_resource = state
            .next_resource
            .checked_add(1)
            .expect("pool identifier space exhausted");
        state.building.insert(id);
        counts.building += 1;
        Some(Acquired::Build(id))
    }

    pub async fn acquire(
        &self,
        cancellation: &Cancellation,
    ) -> Result<Acquired<S::Acquired>, Error> {
        let mut waiter = Waiter {
            core: self,
            ticket: None,
        };
        loop {
            cancellation.check()?;
            event_listener::listener!(self.accounting.changed => listener);
            let acquired = {
                let mut state = self.lock();
                if state.closed {
                    return Err(Error::Closed);
                }
                if waiter.ticket.is_none() && state.waiters.is_empty() {
                    if let Some(acquired) = self.take(&mut state) {
                        return Ok(acquired);
                    }
                }
                if waiter.ticket.is_none() {
                    if state.waiters.len() >= self.options.max_waiters {
                        return Err(Error::WaitersFull);
                    }
                    let ticket = state.next_waiter;
                    state.next_waiter = state
                        .next_waiter
                        .checked_add(1)
                        .expect("pool waiter space exhausted");
                    state.waiters.push_back(ticket);
                    waiter.ticket = Some(ticket);
                }
                if state.waiters.front().copied() == waiter.ticket {
                    self.take(&mut state).inspect(|_| {
                        state.waiters.pop_front();
                        waiter.ticket = None;
                    })
                } else {
                    None
                }
            };
            if let Some(acquired) = acquired {
                self.accounting.notify();
                return Ok(acquired);
            }
            cancellable(listener, cancellation).await?;
        }
    }

    pub fn abort_build(&self, id: ResourceId) {
        let removed = self.lock().building.remove(&id);
        if removed {
            lock(&self.accounting.counts).building -= 1;
        }
        self.accounting.notify();
        self.accounting.retirement_changed.notify(usize::MAX);
    }

    // Failed commits transfer their resource into explicit cleanup ownership.
    pub fn commit(&self, id: ResourceId, slot: S) -> Result<(), Error> {
        let retired = {
            let mut state = self.lock();
            assert!(state.building.remove(&id));
            let mut counts = lock(&self.accounting.counts);
            counts.building -= 1;
            if state.closed {
                counts.retiring += 1;
                Some(slot)
            } else {
                counts.live += 1;
                state.slots.insert(id, slot);
                None
            }
        };
        if let Some(slot) = retired {
            self.retirements.push(id, slot.into_resource());
            Err(Error::Closed)
        } else {
            self.accounting.notify();
            Ok(())
        }
    }

    // Caller holds the state lock. Returns ownership for queueing after unlock.
    pub fn remove_if_drained(&self, state: &mut State<S>, id: ResourceId) -> Option<S> {
        if state
            .slots
            .get(&id)
            .is_some_and(|slot| slot.draining() && slot.uses() == 0)
        {
            let slot = state.slots.remove(&id).unwrap();
            let mut counts = lock(&self.accounting.counts);
            counts.live -= 1;
            counts.retiring += 1;
            Some(slot)
        } else {
            None
        }
    }
    pub fn enqueue(&self, id: ResourceId, slot: Option<S>) {
        if let Some(slot) = slot {
            self.retirements.push(id, slot.into_resource());
        }
        self.accounting.notify();
    }
    pub fn retire(&self, id: ResourceId) -> bool {
        let retired = {
            let mut state = self.lock();
            let Some(slot) = state.slots.get_mut(&id) else {
                return false;
            };
            if slot.draining() {
                return false;
            }
            slot.drain();
            self.remove_if_drained(&mut state, id)
        };
        self.enqueue(id, retired);
        true
    }
    pub fn begin_close(&self) {
        let retired = {
            let mut state = self.lock();
            state.closed = true;
            for slot in state.slots.values_mut() {
                slot.drain();
            }
            let ids: Vec<_> = state.slots.keys().copied().collect();
            ids.into_iter()
                .filter_map(|id| {
                    self.remove_if_drained(&mut state, id)
                        .map(|slot| (id, slot))
                })
                .collect::<Vec<_>>()
        };
        for (id, slot) in retired {
            self.retirements.push(id, slot.into_resource());
        }
        self.accounting.notify();
        self.accounting.retirement_changed.notify(usize::MAX);
    }
    pub fn prune(&self, retain: usize) -> usize {
        let retired = {
            let mut state = self.lock();
            let count = state.slots.len().saturating_sub(retain);
            let ids: Vec<_> = state
                .slots
                .iter()
                .filter(|(_, s)| s.uses() == 0)
                .take(count)
                .map(|(&id, _)| id)
                .collect();
            ids.into_iter()
                .filter_map(|id| {
                    state.slots.get_mut(&id).unwrap().drain();
                    self.remove_if_drained(&mut state, id)
                        .map(|slot| (id, slot))
                })
                .collect::<Vec<_>>()
        };
        let count = retired.len();
        for (id, slot) in retired {
            self.retirements.push(id, slot.into_resource());
        }
        self.accounting.notify();
        count
    }
    pub fn stats(&self) -> Stats {
        let state = self.lock();
        let counts = lock(&self.accounting.counts);
        Stats {
            building: counts.building,
            live: counts.live,
            retiring: counts.retiring,
            draining: state.slots.values().filter(|s| s.draining()).count(),
            uses: state.slots.values().map(|s| s.uses()).sum(),
            available: state.slots.values().filter(|s| s.available()).count(),
            waiters: state.waiters.len(),
            closed: state.closed,
        }
    }
    pub async fn wait_closed(&self, cancel: &Cancellation) -> Result<(), Error> {
        loop {
            cancel.check()?;
            event_listener::listener!(self.accounting.changed => listener);
            if self.is_closed() {
                return Ok(());
            }
            cancellable(listener, cancel).await?;
        }
    }
    fn is_closed(&self) -> bool {
        let state = self.lock();
        state.closed && lock(&self.accounting.counts).total() == 0
    }
    pub async fn next_retirement(
        &self,
        cancel: &Cancellation,
    ) -> Result<Option<Retirement<S::Resource>>, Error> {
        loop {
            cancel.check()?;
            event_listener::listener!(self.accounting.retirement_changed => listener);
            if let Some(item) = self.retirements.pop() {
                return Ok(Some(item));
            }
            if self.is_closed() {
                return Ok(None);
            }
            cancellable(listener, cancel).await?;
        }
    }
}
