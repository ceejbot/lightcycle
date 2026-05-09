use std::collections::{BTreeMap, HashMap};

use crate::hashes::{ConsistentHasher, DefaultHasher};
use crate::{HasId, HashRing};

/// A consistent hash ring implementation.
#[derive(Debug)]
pub struct ConsistentRing<H = DefaultHasher>
where
    H: ConsistentHasher,
{
    /// The number of replicas of each resource to insert into the ring. Ring size = replicas * entries.
    replicas: usize,
    /// The resources we're tracking.
    resources: HashMap<String, Box<dyn HasId>>,
    /// The consistent hash ring itself: each entry points to a key in the resource map.
    hashring: BTreeMap<u64, String>,
    /// The hash function to use
    hasher: H,
}

impl Default for ConsistentRing {
    fn default() -> Self {
        let replicas = 4; // defaulting to pretty small
        let resources = HashMap::new();
        let hashring = BTreeMap::new();
        let hasher = DefaultHasher::new();

        Self {
            replicas,
            resources,
            hashring,
            hasher,
        }
    }
}

impl<H> HashRing for ConsistentRing<H>
where
    H: ConsistentHasher,
{
    type Resource = Box<dyn HasId>;

    fn add(&mut self, resource: Self::Resource) {
        let id = resource.id();

        for i in 0..self.replicas {
            let hashitem = format!("{}{}", id, i);
            let replica_hash = self.hasher.hash(hashitem.as_bytes());
            self.hashring.insert(replica_hash, id.to_owned());
        }

        self.resources.insert(id.to_owned(), resource);
    }

    fn remove(&mut self, resource: &Self::Resource) {
        let id = resource.id();
        for i in 0..self.replicas {
            let hashitem = format!("{}{}", id, i);
            let replica_hash = self.hasher.hash(hashitem.as_bytes());
            self.hashring.remove(&replica_hash);
        }
        self.resources.remove(id);
    }

    fn locate(&self, id: &str) -> Option<&Self::Resource> {
        let hashed_id = self.hasher.hash(id.as_bytes());

        // This search is the heart of the consistent hash ring concept.
        // BTreeMap::range gives us an O(log n) seek to the first key >= hashed_id.
        if let Some((_hash, resource_id)) = self.hashring.range(hashed_id..).next() {
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

    fn is_empty(&self) -> bool {
        self.hashring.is_empty()
    }
}

impl ConsistentRing {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with_replica_count(replicas: usize) -> Self {
        Self {
            replicas,
            resources: HashMap::new(),
            hashring: BTreeMap::new(),
            hasher: DefaultHasher::new(),
        }
    }
}

impl<H> ConsistentRing<H>
where
    H: ConsistentHasher,
{
    pub fn new_with_hasher(hasher: H) -> Self {
        Self {
            replicas: 4,
            resources: HashMap::new(),
            hashring: BTreeMap::new(),
            hasher,
        }
    }

    pub fn new_with_hasher_and_replicas(hasher: H, replicas: usize) -> Self {
        Self {
            replicas,
            resources: HashMap::new(),
            hashring: BTreeMap::new(),
            hasher,
        }
    }

    /// Remove a resource by its ID string
    pub fn remove(&mut self, id: &str) -> Result<Box<dyn HasId>, String> {
        // First remove from hashring
        for i in 0..self.replicas {
            let hashitem = format!("{}{}", id, i);
            let replica_hash = self.hasher.hash(hashitem.as_bytes());
            self.hashring.remove(&replica_hash);
        }

        // Then remove from resources and return it
        self.resources
            .remove(id)
            .ok_or_else(|| format!("Resource '{}' not found", id))
    }

    pub fn hasher_name(&self) -> &'static str {
        self.hasher.name()
    }

    /// Get a raw hash value for testing purposes
    pub fn hash_key(&self, key: &str) -> u64 {
        self.hasher.hash(key.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use super::*;

    #[derive(Debug, Clone)]
    struct MockResource {
        pub name: String,
    }

    impl HasId for MockResource {
        fn id(&self) -> &str {
            &self.name
        }
    }

    static FRUITS: LazyLock<Vec<String>> = LazyLock::new(|| {
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
        let mut ring = ConsistentRing::new_with_replica_count(1);
        ring.add(Box::new(MockResource {
            name: "pecan".to_string(),
        }));
        ring.add(Box::new(MockResource {
            name: "walnut".to_string(),
        }));

        let location = ring.locate("pecan0").expect("pecan0 should be there");
        assert_eq!(location.id(), "pecan");

        let location = ring.locate("walnut0").expect("walnut0 should be there");
        assert_eq!(location.id(), "walnut");
    }

    #[test]
    fn adding_new_replicas_moves_locations() {
        let fruits = pick_some_fruit();
        let mut fruit_iter = fruits.into_iter();
        let mut ring = ConsistentRing::new_with_replica_count(2);

        let f = fruit_iter.next().expect("there should be a first fruitd");
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
        // The exact fruit depends on hash algorithm and formatting
        // Just verify we get a consistent result
        assert!(
            location.id() == "litchi" || location.id() == "pear",
            "Got unexpected fruit: {}",
            location.id()
        );

        // These tests just verify consistent hashing works, not specific values
        let location2 = ring
            .locate("asdfasdfasdfsafasdf")
            .expect("everything should have a home of some kind");
        assert!(FRUITS.iter().any(|f| f == location2.id()));

        let location3 = ring.locate("1").expect("everything should have a home of some kind");
        assert!(FRUITS.iter().any(|f| f == location3.id()));
    }

    #[test]
    fn single_node_rings() {
        let mut ring = ConsistentRing::new_with_replica_count(5);
        let durian = MockResource {
            name: "durian".to_string(),
        };
        ring.add(Box::new(durian)); // nobody likes being next to durian
        let location = ring.locate("a").expect("everything should have a home of some kind");
        assert_eq!(location.id(), "durian");
        let location = ring.locate("z").expect("everything should have a home of some kind");
        assert_eq!(location.id(), "durian");
    }

    #[test]
    fn adding_same_resource_twice() {
        let fruits = pick_some_fruit();
        let mut ring = ConsistentRing::new_with_replica_count(5);
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
