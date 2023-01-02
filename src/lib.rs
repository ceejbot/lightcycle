use std::collections::BTreeMap;
use std::collections::HashMap;

/// When we rebalance, we leave a little room to grow so we don't thrash.
const SIZE_PAD: usize = 16;
/// The replica count also gets some padding.
const REPLICAS_PAD: usize = 8;

/// Things that we can store in the ring must have an ID string they advertise.
pub trait HasId {
    fn id(&self) -> &str;
}

trait HashRing {
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
pub struct LightCycle {
    /// The number of replicas of each resource to insert into the ring. Ring size = replicas * entries.
    replicas: usize,
    /// The number of resources we expect to manage. Helps us decide when to trigger a rebalance.
    size: usize,
    /// The resources we're tracking.
    resources: HashMap<String, Box<dyn HasId>>,
    /// The consistent hash ring itself: each entry points to a key in the resource map.
    hashring: BTreeMap<String, String>,
    /// Each resource has a key per replica. We track those so we can clean up quickly.
    /// This might be a premature optimization preserved from the javascript implementation.
    keycache: HashMap<String, HashMap<usize, String>>,
}

impl Default for LightCycle {
    fn default() -> Self {
        let replicas = 4; // defaulting to pretty small
        let size = 16;
        let keycache = HashMap::new();
        let resources = HashMap::new();
        let hashring = BTreeMap::new();

        Self {
            replicas,
            size,
            keycache,
            resources,
            hashring,
        }
    }
}

impl HashRing for LightCycle {
    type A = Box<dyn HasId>;

    fn add(&mut self, resource: Self::A) {
        let id = resource.id();

        // Note repetition of this code later in rebalance(). TODO refactor.
        let id_cache = self.keycache.entry(id.to_owned()).or_default();

        for i in 0..self.replicas {
            let replica_id = if let Some(cached) = id_cache.get(&i) {
                cached.to_owned()
            } else {
                let hashitem = format!("{}{}", id.to_owned(), i);
                let key = blake3::hash(hashitem.as_bytes()).to_string();
                id_cache.insert(i, key.clone());
                key
            };
            self.hashring.insert(replica_id, id.to_owned());
        }

        self.resources.insert(id.to_owned(), resource);

        if self.resources.len() > self.size {
            self.rebalance();
        }
    }

    fn remove(&mut self, resource: &Self::A) {
        let id = resource.id();

        if let Some(id_cache) = self.keycache.get(id) {
            for value in id_cache.values() {
                self.hashring.remove(value);
            }
        }
        self.keycache.remove(id);
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
            size: replicas + SIZE_PAD,
            keycache: HashMap::new(),
            resources: HashMap::new(),
            hashring: BTreeMap::new(),
        }
    }

    pub fn rebalance(&mut self) {
        let len = self.resources.len();

        self.size = len + SIZE_PAD;
        self.replicas = len + REPLICAS_PAD;
        self.hashring = BTreeMap::new();

        let ids = self.resources.keys().cloned();
        for id in ids {
            // self.add_replicas(&id);
            // I want to write the above instead, but first I must figure out the ownership--
            // we borrow immutably when we get the resource keys.
            let id_cache = self.keycache.entry(id.to_owned()).or_default();

            for i in 0..self.replicas {
                let replica_id = if let Some(cached) = id_cache.get(&i) {
                    cached.to_owned()
                } else {
                    let hashitem = format!("{}{}", id.to_owned(), i);
                    let key = blake3::hash(hashitem.as_bytes()).to_string();
                    id_cache.insert(i, key.clone());
                    key
                };
                self.hashring.insert(replica_id, id.to_owned());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fruit<'a> {
        pub name: &'a str,
    }

    impl HasId for Fruit<'_> {
        fn id(&self) -> &str {
            self.name
        }
    }

    #[test]
    fn distributed_orchard_technology() {
        let apple = Fruit { name: "apple" };
        let kumquat = Fruit { name: "kumquat" };
        let litchi = Fruit { name: "litchi" };
        let papaya = Fruit { name: "papaya" };
        let raspberry = Fruit { name: "raspberry" };

        let mut ring = LightCycle::new_with_replica_count(2);

        ring.add(Box::new(apple));
        assert_eq!(ring.len(), 2);
        assert_eq!(ring.resource_count(), 1);

        // Note that lexical sorting of these names does not matter.
        // We hash them for distribution around the ring.
        ring.add(Box::new(papaya));
        ring.add(Box::new(kumquat));
        ring.add(Box::new(litchi));
        ring.add(Box::new(raspberry));

        assert_eq!(ring.len(), 10);
        assert_eq!(ring.resource_count(), 5);

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
    fn many_boxes_of_fruit() {
        let apple = Fruit { name: "apple" };
        let kumquat = Fruit { name: "kumquat" };
        let litchi = Fruit { name: "litchi" };
        let papaya = Fruit { name: "papaya" };
        let raspberry = Fruit { name: "raspberry" };

        let mut ring = LightCycle::new_with_replica_count(3);

        ring.add(Box::new(apple));
        assert_eq!(ring.len(), 3);
        assert_eq!(ring.resource_count(), 1);

        ring.add(Box::new(papaya));
        ring.add(Box::new(kumquat));
        ring.add(Box::new(litchi));
        ring.add(Box::new(raspberry));

        assert_eq!(ring.len(), 15);
        assert_eq!(ring.resource_count(), 5);

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
}
