// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{
    ResourceId,
    core::{Accounting, lock},
};
use std::{
    collections::VecDeque,
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
};

pub(crate) struct RetirementQueue<R> {
    items: Mutex<VecDeque<(ResourceId, R)>>,
    accounting: Arc<Accounting>,
}
impl<R> RetirementQueue<R> {
    pub fn new(accounting: Arc<Accounting>) -> Self {
        Self {
            items: Mutex::new(VecDeque::new()),
            accounting,
        }
    }
    pub fn push(&self, id: ResourceId, resource: R) {
        lock(&self.items).push_back((id, resource));
        self.accounting.notify();
        self.accounting.retirement_changed.notify(usize::MAX);
    }
    pub fn pop(self: &Arc<Self>) -> Option<Retirement<R>> {
        let item = lock(&self.items).pop_front();
        item.map(|item| Retirement {
            item: Some(item),
            queue: self.clone(),
        })
    }
}

/// A resource charged to the pool until its owner acknowledges cleanup.
/// Await resource-specific retirement through this handle, then call `complete`.
/// Dropping it unfinished requeues it and retains capacity; no task is spawned.
/// Dropping all pool/lease/retirement owners performs only ordinary destruction.
pub struct Retirement<R> {
    item: Option<(ResourceId, R)>,
    queue: Arc<RetirementQueue<R>>,
}
impl<R> Retirement<R> {
    pub fn id(&self) -> ResourceId {
        self.item.as_ref().unwrap().0
    }
    /// Acknowledge completed cleanup, destroy the retained resource, then free
    /// capacity. This call does not perform asynchronous I/O on the resource.
    pub fn complete(mut self) {
        drop(self.item.take().unwrap());
        lock(&self.queue.accounting.counts).retiring -= 1;
        self.queue.accounting.notify();
        self.queue.accounting.retirement_changed.notify(usize::MAX);
    }
}
impl<R> Deref for Retirement<R> {
    type Target = R;
    fn deref(&self) -> &R {
        &self.item.as_ref().unwrap().1
    }
}
impl<R> DerefMut for Retirement<R> {
    fn deref_mut(&mut self) -> &mut R {
        &mut self.item.as_mut().unwrap().1
    }
}
impl<R> Drop for Retirement<R> {
    fn drop(&mut self) {
        if let Some((id, resource)) = self.item.take() {
            self.queue.push(id, resource);
        }
    }
}
