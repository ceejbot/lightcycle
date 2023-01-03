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
    /// The id here is not a resource id, but instead
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
        if let Some((_, resource_id)) = self
            .hashring
            .iter()
            .filter(|(_k, v)| v <= &&id.to_string())
            .last()
        {
            self.resources.get(resource_id)
        } else if let Some((_, resource_id)) = self.hashring.first_key_value() {
            self.resources.get(resource_id)
        } else {
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
            "durian".to_string(),
        ]
    });

    static BERRIES: Lazy<Vec<String>> = Lazy::new(|| {
        vec![
            "banana".to_string(),
            "tomato".to_string(),
            "blueberry".to_string(),
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
    fn consistent_means_consistent() {
        let mut ring = LightCycle::new_with_replica_count(4);
        for name in BERRIES.clone().iter() {
            ring.add(Box::new(MockResource { name: name.clone() }));
        }
        let replica_4 = ring
            .locate("strawberry")
            .expect("we accept strawberries in berry club");

        let mut ring = LightCycle::new_with_replica_count(20);
        for name in BERRIES.clone().iter() {
            ring.add(Box::new(MockResource { name: name.clone() }));
        }

        let replica_20 = ring
            .locate("strawberry")
            .expect("we accept strawberries in berry club");

        assert_eq!(
            replica_4.id(),
            replica_20.id(),
            "location is stable across replications"
        );
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
            eprintln!("adding {}", f.name);
            ring.add(Box::new(f));
        }

        assert_eq!(ring.len(), FRUITS.len() * 2);
        assert_eq!(ring.resource_count(), FRUITS.len());

        let location = ring
            .locate("nom nom nom")
            .expect("everything should have a home of some kind");
        assert_eq!(location.id(), "kumquat");

        let location = ring
            .locate("asdfasdfasdfsafasdf")
            .expect("everything should have a home of some kind");
        assert_eq!(location.id(), "apple");

        let location = ring
            .locate("1")
            .expect("everything should have a home of some kind");
        assert_eq!(location.id(), "papaya");
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
