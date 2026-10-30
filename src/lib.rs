// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Resource admission and lifetime, independent of resource construction and I/O.
#![doc = include_str!("../readme.md")]
#![forbid(unsafe_code)]

mod core;
mod multiplex;
mod retirement;
mod singleplex;

pub use multiplex::{
    Admission, MultiplexAbandonedUse, MultiplexBuildPermit, MultiplexPool, MultiplexReservation,
    MultiplexResource, MultiplexUse, UseId,
};
pub use retirement::Retirement;
pub use singleplex::{SingleplexBuildPermit, SingleplexLease, SingleplexPool, SingleplexResource};

use base_scheduler::Cancellation;
use std::{fmt, future::Future};

/// Hard bounds. Retiring resources continue to occupy resource capacity.
#[derive(Clone, Debug)]
pub struct Options {
    pub max_resources: usize,
    pub max_constructing: usize,
    pub max_waiters: usize,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            max_resources: 32,
            max_constructing: 1,
            max_waiters: 1024,
        }
    }
}
impl Options {
    pub fn validate(&self) -> Result<(), Error> {
        if self.max_resources == 0 || self.max_constructing == 0 {
            Err(Error::InvalidOptions)
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Closed,
    Cancelled,
    WaitersFull,
    AdmissionChanged,
    InvalidOptions,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Closed => "pool admission is closed",
            Self::Cancelled => "pool operation cancelled",
            Self::WaitersFull => "pool waiter limit reached",
            Self::AdmissionChanged => "resource admission changed before use activation",
            Self::InvalidOptions => "resource and construction limits must be positive",
        })
    }
}
impl std::error::Error for Error {}
impl From<base_scheduler::Cancelled> for Error {
    fn from(_: base_scheduler::Cancelled) -> Self {
        Self::Cancelled
    }
}

/// Identifies one registration within a pool; identifiers are never recycled.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ResourceId(pub(crate) u64);

/// An admission opportunity: use an existing resource or construct a new one.
/// A build permit reserves capacity before any asynchronous construction.
#[derive(Debug)]
pub enum Acquisition<P, B> {
    Existing(P),
    Build(B),
}

/// Snapshot of one pool. `uses` includes reserved and abandoned multiplex uses.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Stats {
    pub building: usize,
    pub live: usize,
    pub retiring: usize,
    pub draining: usize,
    pub uses: usize,
    pub available: usize,
    pub waiters: usize,
    pub closed: bool,
}
impl Stats {
    pub fn total(&self) -> usize {
        self.building + self.live + self.retiring
    }
}

/// Shared vocabulary with distinct associated acquisition types.
/// Resource-specific mutation and use completion stay on the concrete types.
pub trait Pool: Send + Sync {
    type Permit: Send;
    type BuildPermit: Send;
    fn acquire_or_reserve(
        &self,
        cancellation: &Cancellation,
    ) -> impl Future<Output = Result<Acquisition<Self::Permit, Self::BuildPermit>, Error>> + Send;
    fn begin_close(&self);
    fn stats(&self) -> Stats;
}
