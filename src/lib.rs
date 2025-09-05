//! A consistent hash ring implementation using configurable hash functions.
//!
//! LightCycle provides a way to distribute items to specific instances of otherwise-identical
//! resources in a balanced way. You have two choices of distribution method: the rendezvous hash,
//! and the more specific case of the consistent hash. The rendezvous hash is in more general use
//! and has better performance characteristics overall, but the consistent hash is still a fun
//! data structure. Both structures can be used for cache sharding, load balancing with session
//! affinity, light cycle distribution, and distributed systems coordination. For the users!

mod consistent;
pub mod hasher;
mod rendezvous;

pub use consistent::ConsistentRing;
use hasher::DefaultHasher;
pub use rendezvous::RendezvousRing;

/// Fun names! Use these or be boring.
pub type LightCycle<H = DefaultHasher> = ConsistentRing<H>;
pub type Recognizer<H = DefaultHasher> = RendezvousRing<H>;
///  Ty curtosis for the name for our error type!
pub type EndOfLine = LightCycleError;

/// Things that we can store in the ring must have an ID string they advertise.
pub trait HasId: std::fmt::Debug + Send + Sync {
    fn id(&self) -> &str;
}

pub trait HashRing {
    /// This type represents the resources we are distributing around the hash ring.
    type Item;

    /// Add a new resource to the hash ring. Stores replica keys distributed around the ring.
    fn add(&mut self, resource: Self::Item);
    /// Remove a resource from the hash ring.
    fn remove(&mut self, resource: &Self::Item);
    /// Given something you want to place on the ring, look up the matching resource to use.
    /// The id here is not a resource id, but instead something that needs to be stored or placed
    /// on one of the managed resources. An example would be a key for a cachable item that you
    /// want to choose a cache resource for.
    fn locate(&self, id: &str) -> Option<&Self::Item>;
    /// Resource count.
    fn resource_count(&self) -> usize;
    /// Total number of entries in the ring.
    fn len(&self) -> usize;
    /// Is the hashring empty?
    fn is_empty(&self) -> bool;

    /// Add a new resource with a specific weight. For algorithms that don't support weighting,
    /// the weight is ignored and this behaves like `add()`.
    fn add_weighted(&mut self, resource: Self::Item, _weight: f64) {
        // Default implementation ignores weight - used by ConsistentRing
        self.add(resource);
    }

    /// Update the weight of an existing resource. Returns an error if the resource is not found
    /// or if the implementation doesn't support weight updates.
    fn update_weight(&mut self, _resource: &Self::Item, _weight: f64) -> Result<(), EndOfLine> {
        // Default implementation returns error - used by ConsistentRing
        Err(EndOfLine::WeightsUnsupported)
    }
}

/// We only have two errors, so let's define them right here.
#[derive(Debug, Clone)]
pub enum LightCycleError {
    NotFound { id: String },
    WeightsUnsupported,
}

impl std::fmt::Display for LightCycleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LightCycleError::NotFound { id } => write!(f, "Resource {id} not found"),
            LightCycleError::WeightsUnsupported => {
                write!(f, "Weight updates not supported by this hash ring implementation")
            }
        }
    }
}
