#![forbid(unsafe_code)]
#![deny(future_incompatible)]
#![warn(
    missing_debug_implementations,
    rust_2018_idioms,
    trivial_casts,
    unused_qualifications
)]

use std::collections::BTreeMap;
use std::collections::HashMap;

/// Things that we can store in the ring must have an ID string they advertise.
pub trait HasId: std::fmt::Debug {
    fn id(&self) -> &str;
}

trait HashRing {
    /// This type represents the resources we are distributing around the hash ring.
    type A;

    /// Add a new resource to the hash ring. Stores replica keys distributed around the ring.
    fn add(&mut self, resource: Self::A);
    /// Remove a resource from the hash ring.
    fn remove(&mut self, resource: &Self::A);
    /// Given something you want to place on the ring, look up the matching resource to use.
    /// The id here is not a resource id, but instead something that needs to be stored or placed
    /// on one of the managed resources. An example would be a key for a cachable item that you
    /// want to choose a cache resource for.
    fn locate(&self, id: &str) -> Option<&Self::A>;
    /// Resource count.
    fn resource_count(&self) -> usize;
    /// Total number of entries in the ring.
    fn len(&self) -> usize;
}

/// A consistent hash ring with blue glowing lights.
#[derive(Debug)]
pub struct LightCycle {
    /// The number of replicas of each resource to insert into the ring. Ring size = replicas * entries.
    replicas: usize,
    /// The resources we're tracking.
    resources: HashMap<String, Box<dyn HasId>>,
    /// The consistent hash ring itself: each entry points to a key in the resource map.
    hashring: BTreeMap<String, String>,
}

impl Default for LightCycle {
    fn default() -> Self {
        let replicas = 4; // defaulting to pretty small
        let resources = HashMap::new();
        let hashring = BTreeMap::new();

        Self {
            replicas,
            resources,
            hashring,
        }
    }
}

impl HashRing for LightCycle {
    type A = Box<dyn HasId>;

    fn add(&mut self, resource: Self::A) {
        let id = resource.id();

        for i in 0..self.replicas {
            let hashitem = format!("{}{}", id.to_owned(), i);
            let replica_id = blake3::hash(hashitem.as_bytes()).to_string();
            self.hashring.insert(replica_id, id.to_owned());
        }

        self.resources.insert(id.to_owned(), resource);
    }

    fn remove(&mut self, resource: &Self::A) {
        let id = resource.id();
        for i in 0..self.replicas {
            let hashitem = format!("{}{}", id.to_owned(), i);
            let replica_id = blake3::hash(hashitem.as_bytes()).to_string();
            self.hashring.remove(&replica_id);
        }
        self.resources.remove(id);
    }

    fn locate(&self, id: &str) -> Option<&Self::A> {
        let hashed_id = blake3::hash(id.as_bytes()).to_string();

        // This search is the heart of the consistent hash ring concept.
        // The data structure we use for the hashring has to be something
        // that maintains a lexical ordering and lets us do this search.
        if let Some((_hash, resource_id)) = self.hashring.iter().find(|(k, _v)| k >= &&hashed_id) {
            self.resources.get(resource_id)
        } else if let Some((_hash, resource_id)) = self.hashring.last_key_value() {
            // We're past the end, so we take the last node.
            self.resources.get(resource_id)
        } else {
            // This case happens if the ring is empty. People who do that get what they deserve.
            None
        }
    }

    fn resource_count(&self) -> usize {
        self.resources.len()
    }

    fn len(&self) -> usize {
        self.hashring.len()
    }
}

impl LightCycle {
    pub fn new_with_replica_count(replicas: usize) -> Self {
        Self {
            replicas,
            resources: HashMap::new(),
            hashring: BTreeMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use once_cell::sync::Lazy;

    #[derive(Debug, Clone)]
    struct MockResource {
        pub name: String,
    }

    impl HasId for MockResource {
        fn id(&self) -> &str {
            &self.name
        }
    }

    static FRUITS: Lazy<Vec<String>> = Lazy::new(|| {
        vec![
            "apple".to_string(),
            "kumquat".to_string(),
            "litchi".to_string(),
            "papaya".to_string(),
            "pear".to_string(),
            "mangosteen".to_string(),
            "orange".to_string(),
        ]
    });

    fn pick_some_fruit() -> Vec<MockResource> {
        let mut result = Vec::new();
        for name in FRUITS.iter() {
            result.push(MockResource { name: name.clone() });
        }

        result
    }

    #[test]
    fn locations_behave_as_expected() {
        // This test knows about how we generate id hashes.
        // First, make a zero-replicas ring.
        let mut ring = LightCycle::new_with_replica_count(1);
        ring.add(Box::new(MockResource {
            name: "pecan".to_string(),
        }));
        ring.add(Box::new(MockResource {
            name: "walnut".to_string(),
        }));

        let location = ring.locate("pecan0").unwrap();
        assert_eq!(location.id(), "pecan");

        let location = ring.locate("walnut0").unwrap();
        assert_eq!(location.id(), "walnut");
    }

    #[test]
    fn adding_new_replicas_moves_locations() {
        let fruits = pick_some_fruit();
        let mut fruit_iter = fruits.into_iter();
        let mut ring = LightCycle::new_with_replica_count(2);

        let f = fruit_iter.next().unwrap();
        ring.add(Box::new(f));
        assert_eq!(ring.len(), 2);
        assert_eq!(ring.resource_count(), 1);

        let location = ring
            .locate("nom nom nom")
            .expect("everything should have a home of some kind");
        assert_eq!(location.id(), "apple");

        for f in fruit_iter {
            ring.add(Box::new(f));
        }

        assert_eq!(ring.len(), FRUITS.len() * 2);
        assert_eq!(ring.resource_count(), FRUITS.len());

        let location = ring
            .locate("nom nom nom")
            .expect("everything should have a home of some kind");
        assert_eq!(location.id(), "pear");

        let location = ring
            .locate("asdfasdfasdfsafasdf")
            .expect("everything should have a home of some kind");
        assert_eq!(location.id(), "orange");

        let location = ring
            .locate("1")
            .expect("everything should have a home of some kind");
        assert_eq!(location.id(), "mangosteen");
    }

    #[test]
    fn single_node_rings() {
        let mut ring = LightCycle::new_with_replica_count(5);
        let durian = MockResource {
            name: "durian".to_string(),
        };
        ring.add(Box::new(durian)); // nobody likes being next to durian
        let location = ring
            .locate("a")
            .expect("everything should have a home of some kind");
        assert_eq!(location.id(), "durian");
        let location = ring
            .locate("z")
            .expect("everything should have a home of some kind");
        assert_eq!(location.id(), "durian");
    }

    #[test]
    fn adding_same_resource_twice() {
        let fruits = pick_some_fruit();
        let mut ring = LightCycle::new_with_replica_count(5);
        for f in fruits.clone().into_iter() {
            ring.add(Box::new(f));
        }
        assert_eq!(ring.len(), FRUITS.len() * 5);
        assert_eq!(ring.resource_count(), FRUITS.len());

        for f in fruits.into_iter() {
            ring.add(Box::new(f));
        }
        assert_eq!(
            ring.len(),
            FRUITS.len() * 5,
            "adding resources we already have should be a no-op"
        );
        assert_eq!(
            ring.resource_count(),
            FRUITS.len(),
            "adding resources we already have should be a no-op"
        );
    }
}
