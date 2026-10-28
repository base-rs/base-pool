// Released under the MIT License.
// Copyright, 2019-2026, by Samuel Williams.

use crate::{
    Acquisition, Error, Options, Pool, ResourceId, Retirement, Stats,
    core::{Acquired, Core, Slot},
};
use base_scheduler::Cancellation;
use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, Weak},
};

struct SingleSlot<R> {
    resource: Option<R>,
    draining: bool,
}
impl<R: Send> Slot for SingleSlot<R> {
    type Resource = R;
    type Acquired = R;
    fn available(&self) -> bool {
        !self.draining && self.resource.is_some()
    }
    fn acquire(&mut self) -> R {
        self.resource.take().unwrap()
    }
    fn uses(&self) -> usize {
        usize::from(self.resource.is_none())
    }
    fn draining(&self) -> bool {
        self.draining
    }
    fn drain(&mut self) {
        self.draining = true;
    }
    fn into_resource(self) -> R {
        self.resource.unwrap()
    }
}

/// Exclusive resource ownership. Resources need `Send`, without requiring `Sync`.
/// Clean reuse is an explicit lease release; unfinished lease drop retires it.
pub struct SingleplexPool<R: Send> {
    core: Arc<Core<SingleSlot<R>>>,
}
impl<R: Send> Clone for SingleplexPool<R> {
    fn clone(&self) -> Self {
        Self {
            core: self.core.clone(),
        }
    }
}
impl<R: Send> SingleplexPool<R> {
    pub fn new(options: Options) -> Result<Self, Error> {
        Ok(Self {
            core: Core::new(options)?,
        })
    }
    pub async fn acquire_or_reserve(
        &self,
        cancel: &Cancellation,
    ) -> Result<Acquisition<SingleplexLease<R>, SingleplexBuildPermit<R>>, Error> {
        Ok(match self.core.acquire(cancel).await? {
            Acquired::Existing(id, resource) => Acquisition::Existing(SingleplexLease {
                core: self.core.clone(),
                id,
                resource: Some(resource),
            }),
            Acquired::Build(id) => Acquisition::Build(SingleplexBuildPermit {
                core: self.core.clone(),
                id,
                pending: true,
            }),
        })
    }
    pub fn begin_close(&self) {
        self.core.begin_close();
    }
    pub fn stats(&self) -> Stats {
        self.core.stats()
    }
    /// Retire unused registrations until at most `retain` live resources remain,
    /// or until only in-use resources remain. No I/O occurs here.
    pub fn prune(&self, retain: usize) -> usize {
        self.core.prune(retain)
    }
    pub fn try_retirement(&self) -> Option<Retirement<R>> {
        self.core.retirements.pop()
    }
    /// Wait for cleanup work; `None` means admission is closed and all resources
    /// and construction/retirement obligations have finished.
    pub async fn next_retirement(
        &self,
        cancel: &Cancellation,
    ) -> Result<Option<Retirement<R>>, Error> {
        self.core.next_retirement(cancel).await
    }
    /// Call `begin_close`, finish active work and drain retirements concurrently.
    /// This waits for acknowledgments; it does not execute resource cleanup.
    pub async fn wait_closed(&self, cancel: &Cancellation) -> Result<(), Error> {
        self.core.wait_closed(cancel).await
    }
}
impl<R: Send> Pool for SingleplexPool<R> {
    type Permit = SingleplexLease<R>;
    type BuildPermit = SingleplexBuildPermit<R>;
    async fn acquire_or_reserve(
        &self,
        cancel: &Cancellation,
    ) -> Result<Acquisition<Self::Permit, Self::BuildPermit>, Error> {
        self.acquire_or_reserve(cancel).await
    }
    fn begin_close(&self) {
        self.begin_close();
    }
    fn stats(&self) -> Stats {
        self.stats()
    }
}

/// A reserved construction slot. Register the fully established resource, then
/// acquire again to use it. Driver registration must precede this commit.
/// Dropping a permit returns its construction reservation exactly once.
pub struct SingleplexBuildPermit<R: Send> {
    core: Arc<Core<SingleSlot<R>>>,
    id: ResourceId,
    pending: bool,
}
impl<R: Send> SingleplexBuildPermit<R> {
    pub fn id(&self) -> ResourceId {
        self.id
    }
    /// A commit racing shutdown transfers the resource into the retirement queue
    /// and returns `Closed`; the cleanup owner must still drain that queue.
    pub fn commit(mut self, resource: R) -> Result<SingleplexResource<R>, Error> {
        self.core.commit(
            self.id,
            SingleSlot {
                resource: Some(resource),
                draining: false,
            },
        )?;
        self.pending = false;
        Ok(SingleplexResource {
            core: Arc::downgrade(&self.core),
            id: self.id,
        })
    }
}
impl<R: Send> Drop for SingleplexBuildPermit<R> {
    fn drop(&mut self) {
        if self.pending {
            self.core.abort_build(self.id);
        }
    }
}

/// A weak registration handle for health changes and retirement requests.
/// Holding it does not keep the pool alive or give access to a leased resource.
pub struct SingleplexResource<R: Send> {
    core: Weak<Core<SingleSlot<R>>>,
    id: ResourceId,
}
impl<R: Send> Clone for SingleplexResource<R> {
    fn clone(&self) -> Self {
        Self {
            core: self.core.clone(),
            id: self.id,
        }
    }
}
impl<R: Send> SingleplexResource<R> {
    pub fn id(&self) -> ResourceId {
        self.id
    }
    /// Stop admission immediately; an outstanding lease retains cleanup ownership
    /// until it returns. Returns false for an already-draining/removed resource.
    pub fn retire(&self) -> bool {
        self.core.upgrade().is_some_and(|core| core.retire(self.id))
    }
}

/// Exclusive non-cloneable resource lease. Release only after proving that the
/// whole operation is synchronized and reusable, including both HTTP directions.
pub struct SingleplexLease<R: Send> {
    core: Arc<Core<SingleSlot<R>>>,
    id: ResourceId,
    resource: Option<R>,
}
impl<R: Send> SingleplexLease<R> {
    pub fn id(&self) -> ResourceId {
        self.id
    }
    pub fn release(mut self) {
        self.finish(false);
    }
    pub fn retire(mut self) {
        self.finish(true);
    }
    fn finish(&mut self, retire: bool) {
        let Some(resource) = self.resource.take() else {
            return;
        };
        let retired = {
            let mut state = self.core.lock();
            let slot = state.slots.get_mut(&self.id).unwrap();
            if retire {
                slot.drain();
            }
            debug_assert!(slot.resource.is_none());
            slot.resource = Some(resource);
            self.core.remove_if_drained(&mut state, self.id)
        };
        self.core.enqueue(self.id, retired);
    }
}
impl<R: Send> Deref for SingleplexLease<R> {
    type Target = R;
    fn deref(&self) -> &R {
        self.resource.as_ref().unwrap()
    }
}
impl<R: Send> DerefMut for SingleplexLease<R> {
    fn deref_mut(&mut self) -> &mut R {
        self.resource.as_mut().unwrap()
    }
}
impl<R: Send> Drop for SingleplexLease<R> {
    fn drop(&mut self) {
        self.finish(true);
    }
}
