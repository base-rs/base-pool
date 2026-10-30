// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{
    Acquisition, Error, Options, Pool, ResourceId, Retirement, Stats,
    core::{Acquired, Core, Slot, cancellable},
};
use base_scheduler::Cancellation;
use std::{
    ops::Deref,
    sync::{Arc, Weak},
};

/// Concurrent-use bound and optional consumable opening credit. Completing a
/// use restores concurrent capacity, but never manufactures opening credit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Admission {
    pub max_concurrent: usize,
    pub opening_credit: Option<usize>,
}
impl Admission {
    pub fn concurrent(max_concurrent: usize) -> Self {
        Self {
            max_concurrent,
            opening_credit: None,
        }
    }
}

/// A use identifier within one resource registration, never recycled there.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct UseId(u64);

#[derive(Clone, Copy, Eq, PartialEq)]
enum Stage {
    Reserved,
    Active,
    Abandoned,
    Cleaning,
}

// Use identifiers are allocated in increasing order, so pushing keeps the
// list sorted; it retains its allocation across uses.
struct Uses(Vec<(UseId, Stage)>);
impl Uses {
    fn len(&self) -> usize {
        self.0.len()
    }
    fn position(&self, id: &UseId) -> Option<usize> {
        self.0.binary_search_by(|(use_id, _)| use_id.cmp(id)).ok()
    }
    fn get_mut(&mut self, id: &UseId) -> Option<&mut Stage> {
        self.position(id).map(|index| &mut self.0[index].1)
    }
    fn remove(&mut self, id: &UseId) -> Option<Stage> {
        self.position(id).map(|index| self.0.remove(index).1)
    }
}

struct MultiSlot<R> {
    resource: Arc<R>,
    admission: Admission,
    uses: Uses,
    reserved: usize,
    active: usize,
    next_use: u64,
    draining: bool,
}
impl<R: Send + Sync> Slot for MultiSlot<R> {
    type Resource = Arc<R>;
    type Acquired = (Arc<R>, UseId);
    fn available(&self) -> bool {
        !self.draining
            && self.uses.len() < self.admission.max_concurrent
            && self
                .admission
                .opening_credit
                .is_none_or(|credit| self.reserved < credit)
    }
    fn acquire(&mut self) -> Self::Acquired {
        let id = UseId(self.next_use);
        self.next_use = self
            .next_use
            .checked_add(1)
            .expect("multiplex use identifier space exhausted");
        self.uses.0.push((id, Stage::Reserved));
        self.reserved += 1;
        (self.resource.clone(), id)
    }
    fn uses(&self) -> usize {
        self.uses.len()
    }
    fn draining(&self) -> bool {
        self.draining
    }
    fn drain(&mut self) {
        self.draining = true;
    }
    fn into_resource(self) -> Arc<R> {
        self.resource
    }
}

/// Shared resources with non-cloneable use reservations. The protocol owner
/// validates and activates reservations while committing the actual operation.
pub struct MultiplexPool<R: Send + Sync> {
    core: Arc<Core<MultiSlot<R>>>,
}
impl<R: Send + Sync> Clone for MultiplexPool<R> {
    fn clone(&self) -> Self {
        Self {
            core: self.core.clone(),
        }
    }
}
impl<R: Send + Sync> MultiplexPool<R> {
    pub fn new(options: Options) -> Result<Self, Error> {
        Ok(Self {
            core: Core::new(options)?,
        })
    }
    pub async fn acquire_or_reserve(
        &self,
        cancel: &Cancellation,
    ) -> Result<Acquisition<MultiplexReservation<R>, MultiplexBuildPermit<R>>, Error> {
        Ok(match self.core.acquire(cancel).await? {
            Acquired::Existing(id, (resource, use_id)) => {
                Acquisition::Existing(MultiplexReservation {
                    core: self.core.clone(),
                    id,
                    use_id,
                    resource,
                    pending: true,
                })
            }
            Acquired::Build(id) => Acquisition::Build(MultiplexBuildPermit {
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
    pub fn prune(&self, retain: usize) -> usize {
        self.core.prune(retain)
    }
    pub fn try_retirement(&self) -> Option<Retirement<Arc<R>>> {
        self.core.retirements.pop()
    }
    pub async fn next_retirement(
        &self,
        cancel: &Cancellation,
    ) -> Result<Option<Retirement<Arc<R>>>, Error> {
        self.core.next_retirement(cancel).await
    }
    pub async fn wait_closed(&self, cancel: &Cancellation) -> Result<(), Error> {
        self.core.wait_closed(cancel).await
    }
}
impl<R: Send + Sync> Pool for MultiplexPool<R> {
    type Permit = MultiplexReservation<R>;
    type BuildPermit = MultiplexBuildPermit<R>;
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

/// Reserves resource construction, independent of per-resource stream capacity.
/// Commit registers the shared handle; acquire again for a use reservation.
pub struct MultiplexBuildPermit<R: Send + Sync> {
    core: Arc<Core<MultiSlot<R>>>,
    id: ResourceId,
    pending: bool,
}
impl<R: Send + Sync> MultiplexBuildPermit<R> {
    pub fn id(&self) -> ResourceId {
        self.id
    }
    /// Register only after installing the resource's progress owner. Zero stream
    /// capacity is valid and can be increased later through the returned handle.
    /// A late commit after shutdown enters retirement and returns `Closed`.
    pub fn commit(
        mut self,
        resource: R,
        admission: Admission,
    ) -> Result<MultiplexResource<R>, Error> {
        let slot = MultiSlot {
            resource: Arc::new(resource),
            admission,
            uses: Uses(Vec::new()),
            reserved: 0,
            active: 0,
            next_use: 0,
            draining: false,
        };
        self.core.commit(self.id, slot)?;
        self.pending = false;
        Ok(MultiplexResource {
            core: Arc::downgrade(&self.core),
            id: self.id,
        })
    }
}
impl<R: Send + Sync> Drop for MultiplexBuildPermit<R> {
    fn drop(&mut self) {
        if self.pending {
            self.core.abort_build(self.id);
        }
    }
}

/// Weak driver-side control of one registration. Capacity updates and retirement
/// wake acquirers even when no use has completed. No shared mutable resource
/// reference is exposed by the pool.
pub struct MultiplexResource<R: Send + Sync> {
    core: Weak<Core<MultiSlot<R>>>,
    id: ResourceId,
}
impl<R: Send + Sync> Clone for MultiplexResource<R> {
    fn clone(&self) -> Self {
        Self {
            core: self.core.clone(),
            id: self.id,
        }
    }
}
impl<R: Send + Sync> MultiplexResource<R> {
    pub fn id(&self) -> ResourceId {
        self.id
    }
    pub fn retire(&self) -> bool {
        self.core.upgrade().is_some_and(|core| core.retire(self.id))
    }
    /// Publish current protocol/local admission. Decreases do not terminate uses
    /// already active; outstanding reservations are revalidated at activation.
    pub fn update(&self, admission: Admission) -> bool {
        let Some(core) = self.core.upgrade() else {
            return false;
        };
        {
            let mut state = core.lock();
            let Some(slot) = state.slots.get_mut(&self.id) else {
                return false;
            };
            if slot.draining {
                return false;
            }
            slot.admission = admission;
            state.released(self.id);
        }
        core.accounting.notify();
        true
    }
    pub fn try_abandoned(&self) -> Option<MultiplexAbandonedUse<R>> {
        let core = self.core.upgrade()?;
        let (use_id, resource) = {
            let mut state = core.lock();
            let slot = state.slots.get_mut(&self.id)?;
            let (use_id, stage) = slot
                .uses
                .0
                .iter_mut()
                .find(|(_, stage)| *stage == Stage::Abandoned)?;
            *stage = Stage::Cleaning;
            (*use_id, slot.resource.clone())
        };
        Some(MultiplexAbandonedUse {
            core,
            id: self.id,
            use_id,
            resource,
            pending: true,
        })
    }
    /// The driver receives abandoned uses, performs protocol cleanup/reset, then
    /// acknowledges completion. `None` means the registration has retired.
    pub async fn next_abandoned(
        &self,
        cancel: &Cancellation,
    ) -> Result<Option<MultiplexAbandonedUse<R>>, Error> {
        loop {
            cancel.check()?;
            let Some(core) = self.core.upgrade() else {
                return Ok(None);
            };
            event_listener::listener!(core.accounting.changed => listener);
            if let Some(abandoned) = self.try_abandoned() {
                return Ok(Some(abandoned));
            }
            if !core.lock().slots.contains_key(&self.id) {
                return Ok(None);
            }
            cancellable(listener, cancel).await?;
        }
    }
}

/// Permission to open a use, with shared read-only access to its resource handle.
/// Dropping before activation returns admission immediately: no use was opened.
pub struct MultiplexReservation<R: Send + Sync> {
    core: Arc<Core<MultiSlot<R>>>,
    id: ResourceId,
    use_id: UseId,
    resource: Arc<R>,
    pending: bool,
}
impl<R: Send + Sync> MultiplexReservation<R> {
    pub fn id(&self) -> UseId {
        self.use_id
    }
    pub fn resource_id(&self) -> ResourceId {
        self.id
    }
    /// Revalidate pool admission at the protocol owner's opening boundary.
    /// This accounts for an active use; it does not send frames or establish
    /// peer acceptance. The protocol must still validate its own opening state.
    pub fn activate(mut self) -> Result<MultiplexUse<R>, Error> {
        let (accepted, retired) = {
            let mut state = self.core.lock();
            let closed = state.closed;
            let slot = state.slots.get_mut(&self.id).unwrap();
            let stage = slot.uses.get_mut(&self.use_id).unwrap();
            assert!(*stage == Stage::Reserved);
            slot.reserved -= 1;
            let accepted = !closed
                && !slot.draining
                && slot.active < slot.admission.max_concurrent
                && slot.admission.opening_credit != Some(0);
            if accepted {
                // Activation changes the existing obligation in place.
                *stage = Stage::Active;
                slot.active += 1;
                if let Some(credit) = &mut slot.admission.opening_credit {
                    *credit -= 1;
                }
            } else {
                slot.uses.remove(&self.use_id);
            }
            (accepted, self.core.remove_if_drained(&mut state, self.id))
        };
        self.pending = false;
        if !accepted {
            self.core.enqueue(self.id, retired);
            return Err(Error::AdmissionChanged);
        }
        debug_assert!(retired.is_none());
        Ok(MultiplexUse {
            core: self.core.clone(),
            id: self.id,
            use_id: self.use_id,
            resource: self.resource.clone(),
            pending: true,
        })
    }
}
impl<R: Send + Sync> Deref for MultiplexReservation<R> {
    type Target = R;
    fn deref(&self) -> &R {
        &self.resource
    }
}
impl<R: Send + Sync> Drop for MultiplexReservation<R> {
    fn drop(&mut self) {
        if !self.pending {
            return;
        }
        let retired = {
            let mut state = self.core.lock();
            let slot = state.slots.get_mut(&self.id).unwrap();
            assert!(slot.uses.remove(&self.use_id) == Some(Stage::Reserved));
            slot.reserved -= 1;
            self.core.remove_if_drained(&mut state, self.id)
        };
        self.core.enqueue(self.id, retired);
    }
}

fn finish<R: Send + Sync>(core: &Core<MultiSlot<R>>, id: ResourceId, use_id: UseId) {
    let retired = {
        let mut state = core.lock();
        let slot = state.slots.get_mut(&id).unwrap();
        let stage = slot.uses.remove(&use_id).unwrap();
        assert!(stage == Stage::Active || stage == Stage::Cleaning);
        slot.active -= 1;
        core.remove_if_drained(&mut state, id)
    };
    core.enqueue(id, retired);
}
fn abandon<R: Send + Sync>(core: &Core<MultiSlot<R>>, id: ResourceId, use_id: UseId) {
    {
        let mut state = core.lock();
        let stage = state
            .slots
            .get_mut(&id)
            .unwrap()
            .uses
            .get_mut(&use_id)
            .unwrap();
        assert!(*stage == Stage::Active || *stage == Stage::Cleaning);
        *stage = Stage::Abandoned;
    }
    core.accounting.notify();
}

/// The non-cloneable completion obligation for an opened use. Transfer it to
/// the exchange owner and complete only after both directions/protocol cleanup.
/// Drop reports abandonment to the resource driver and retains the charge.
pub struct MultiplexUse<R: Send + Sync> {
    core: Arc<Core<MultiSlot<R>>>,
    id: ResourceId,
    use_id: UseId,
    resource: Arc<R>,
    pending: bool,
}
impl<R: Send + Sync> MultiplexUse<R> {
    pub fn id(&self) -> UseId {
        self.use_id
    }
    pub fn resource_id(&self) -> ResourceId {
        self.id
    }
    pub fn complete(mut self) {
        finish(&self.core, self.id, self.use_id);
        self.pending = false;
    }
}
impl<R: Send + Sync> Deref for MultiplexUse<R> {
    type Target = R;
    fn deref(&self) -> &R {
        &self.resource
    }
}
impl<R: Send + Sync> Drop for MultiplexUse<R> {
    fn drop(&mut self) {
        if self.pending {
            abandon(&self.core, self.id, self.use_id);
        }
    }
}

/// Driver-owned cleanup of one abandoned use. Complete after protocol cleanup;
/// dropping this handle reports it again without releasing its charge.
pub struct MultiplexAbandonedUse<R: Send + Sync> {
    core: Arc<Core<MultiSlot<R>>>,
    id: ResourceId,
    use_id: UseId,
    resource: Arc<R>,
    pending: bool,
}
impl<R: Send + Sync> MultiplexAbandonedUse<R> {
    pub fn id(&self) -> UseId {
        self.use_id
    }
    pub fn resource_id(&self) -> ResourceId {
        self.id
    }
    pub fn complete(mut self) {
        finish(&self.core, self.id, self.use_id);
        self.pending = false;
    }
}
impl<R: Send + Sync> Deref for MultiplexAbandonedUse<R> {
    type Target = R;
    fn deref(&self) -> &R {
        &self.resource
    }
}
impl<R: Send + Sync> Drop for MultiplexAbandonedUse<R> {
    fn drop(&mut self) {
        if self.pending {
            abandon(&self.core, self.id, self.use_id);
        }
    }
}
