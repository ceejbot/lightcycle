//! The rendezvous hash data structure.
//!
//! The rendezvous hashing algorithm (also known as Highest Random Weight)
/// assigns each key to the node with the highest `hash(key + node_id) * weight` value.
/// This distributes the keys fairly by default while allowing you to bias the distribution
/// toward weighted nodes if you want. You might, for example, have a node that has more memory capacity
/// or a larger disk, and want to store more items there.
use crate::hashes::{ConsistentHasher, DefaultHasher};
use crate::{EndOfLine, HasId, HashRing};

/// A rendezvous hash ring implementation with support for weighted nodes.
#[derive(Debug)]
pub struct RendezvousRing<H = DefaultHasher>
where
    H: ConsistentHasher,
{
    /// The resources and their weights: (resource, weight)
    resources: Vec<(Box<dyn HasId>, f64)>,
    /// The hash function to use for rendezvous hashing
    algorithm: H,
}

impl<H> Default for RendezvousRing<H>
where
    H: ConsistentHasher,
{
    fn default() -> Self {
        Self {
            resources: Vec::new(),
            algorithm: H::new(),
        }
    }
}

impl<H> HashRing for RendezvousRing<H>
where
    H: ConsistentHasher,
{
    type Resource = Box<dyn HasId>;

    fn add(&mut self, item: Self::Resource) {
        // Default weight of 1.0 for unweighted adds
        self.add_weighted(item, 1.0);
    }

    fn remove(&mut self, resource: &Self::Resource) {
        let id = resource.id();
        self.resources.retain(|(res, _)| res.id() != id);
    }

    fn locate(&self, id: &str) -> Option<&Self::Resource> {
        if self.resources.is_empty() {
            return None;
        }

        let mut best_node: Option<&Box<dyn HasId>> = None;
        let mut best_score = f64::NEG_INFINITY;

        for (resource, weight) in &self.resources {
            // Rendezvous algorithm: hash(key + node_id) * weight
            let combined_key = format!("{}{}", id, resource.id());
            let hash_value = self.algorithm.hash(combined_key.as_bytes());

            // Normalize hash to [0,1] then apply weight using power scaling
            // This uses the weighted rendezvous hashing approach: hash^(1/weight)
            let normalized_hash = hash_value as f64 / u64::MAX as f64;
            let score = if *weight > 0.0 {
                normalized_hash.powf(1.0 / weight)
            } else {
                f64::NEG_INFINITY // Invalid weight
            };

            if score > best_score {
                best_score = score;
                best_node = Some(resource);
            }
        }

        best_node
    }

    fn resource_count(&self) -> usize {
        self.resources.len()
    }

    fn len(&self) -> usize {
        // For rendezvous hashing, length equals resource count (no replicas).
        // This is the simplification over the consistent hash.
        self.resources.len()
    }

    fn is_empty(&self) -> bool {
        self.resources.is_empty()
    }

    fn add_weighted(&mut self, resource: Self::Resource, weight: f64) {
        let id = resource.id().to_string();

        // Remove any existing resource with the same ID first
        self.resources.retain(|(res, _)| res.id() != id);

        // Add the new resource with its weight
        self.resources.push((resource, weight));
    }

    fn update_weight(&mut self, resource: &Self::Resource, weight: f64) -> Result<(), EndOfLine> {
        let id = resource.id();

        for (res, current_weight) in &mut self.resources {
            if res.id() == id {
                *current_weight = weight;
                return Ok(());
            }
        }

        Err(EndOfLine::NotFound { id: id.to_string() })
    }
}

impl RendezvousRing {
    /// Create a new rendezvous hash ring with the default hash algorithm
    pub fn new() -> Self {
        Self::default()
    }
}

impl<H> RendezvousRing<H>
where
    H: ConsistentHasher,
{
    /// Create a new rendezvous hash ring with a specific algorithm
    pub fn new_with_hasher(hasher: H) -> Self {
        Self {
            resources: Vec::new(),
            algorithm: hasher,
        }
    }

    /// Get the hash algorithm name for diagnostics
    pub fn hash_algorithm_name(&self) -> &'static str {
        self.algorithm.name()
    }

    /// Get a raw hash value for testing purposes
    pub fn hash_key(&self, key: &str) -> u64 {
        self.algorithm.hash(key.as_bytes())
    }

    /// Get the weight of a resource, if it exists
    pub fn get_weight(&self, resource: &dyn HasId) -> Option<f64> {
        let id = resource.id();
        self.resources
            .iter()
            .find(|(res, _)| res.id() == id)
            .map(|(_, weight)| *weight)
    }

    /// List all resources and their weights
    pub fn resources_with_weights(&self) -> Vec<(&dyn HasId, f64)> {
        self.resources
            .iter()
            .map(|(res, weight)| (res.as_ref(), *weight))
            .collect()
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
    fn rendezvous_hash_basic_functionality() {
        let mut ring = RendezvousRing::new();
        let resources = pick_some_fruit();

        // Add resources
        for resource in &resources {
            ring.add(Box::new(resource.clone()));
        }

        // Test locate
        let key = "test-key";
        let located = ring.locate(key);
        assert!(located.is_some());
        let first_location = located.expect("should have location").id();

        // Test consistency - same key should always map to same resource
        for _ in 0..10 {
            let located = ring.locate(key).expect("should have location");
            assert_eq!(located.id(), first_location);
        }

        // Test different keys map to different resources (mostly)
        let mut locations = std::collections::HashSet::new();
        for i in 0..100 {
            let key = format!("key-{}", i);
            if let Some(resource) = ring.locate(&key) {
                locations.insert(resource.id().to_string());
            }
        }
        // Should use multiple resources
        assert!(locations.len() > 1);
    }

    #[test]
    fn rendezvous_hash_empty_ring() {
        let ring = RendezvousRing::new();
        assert_eq!(ring.resource_count(), 0);
        assert!(ring.is_empty());
        assert!(ring.locate("any-key").is_none());
    }

    #[test]
    fn rendezvous_hash_single_node() {
        let mut ring = RendezvousRing::new();
        let resource = MockResource {
            name: "single".to_string(),
        };
        ring.add(Box::new(resource));

        assert_eq!(ring.resource_count(), 1);
        assert!(!ring.is_empty());

        // Everything should map to the single node
        for i in 0..10 {
            let key = format!("key-{}", i);
            let located = ring.locate(&key).expect("should find single node");
            assert_eq!(located.id(), "single");
        }
    }

    #[test]
    fn rendezvous_hash_remove_resources() {
        let mut ring = RendezvousRing::new();
        let resources = pick_some_fruit();

        // Add all resources
        for resource in &resources {
            ring.add(Box::new(resource.clone()));
        }
        assert_eq!(ring.resource_count(), resources.len());

        // Remove one resource
        let to_remove = &resources[0];
        let boxed_remove: Box<dyn HasId> = Box::new(to_remove.clone());
        ring.remove(&boxed_remove);
        assert_eq!(ring.resource_count(), resources.len() - 1);

        // Verify it's not returned by locate
        for i in 0..100 {
            let key = format!("key-{}", i);
            if let Some(located) = ring.locate(&key) {
                assert_ne!(located.id(), to_remove.id());
            }
        }
    }

    #[test]
    fn rendezvous_hash_adding_same_resource_replaces() {
        let mut ring = RendezvousRing::new();

        let resource1 = MockResource {
            name: "duplicate".to_string(),
        };
        let resource2 = MockResource {
            name: "duplicate".to_string(),
        };

        ring.add(Box::new(resource1));
        assert_eq!(ring.resource_count(), 1);

        // Adding same ID should replace, not add
        ring.add(Box::new(resource2));
        assert_eq!(ring.resource_count(), 1);
    }

    #[test]
    fn rendezvous_hash_weighted_resources() {
        let mut ring = RendezvousRing::new();

        let light = MockResource {
            name: "light".to_string(),
        };
        let heavy = MockResource {
            name: "heavy".to_string(),
        };

        // Add with different weights
        ring.add_weighted(Box::new(light.clone()), 1.0);
        ring.add_weighted(Box::new(heavy.clone()), 10.0);

        // Count distribution
        let mut light_count = 0;
        let mut heavy_count = 0;

        for i in 0..1000 {
            let key = format!("test-key-{}", i);
            if let Some(located) = ring.locate(&key) {
                if located.id() == "light" {
                    light_count += 1;
                } else {
                    heavy_count += 1;
                }
            }
        }

        // Heavy should get significantly more keys
        assert!(heavy_count > light_count * 5);
    }

    #[test]
    fn rendezvous_hash_weight_updates() {
        let mut ring = RendezvousRing::new();

        let resource = MockResource {
            name: "weighted".to_string(),
        };
        let boxed_resource: Box<dyn HasId> = Box::new(resource.clone());

        // Add with initial weight
        ring.add_weighted(Box::new(resource.clone()), 5.0);
        assert_eq!(ring.get_weight(boxed_resource.as_ref()), Some(5.0));

        // Update weight
        assert!(ring.update_weight(&boxed_resource, 10.0).is_ok());
        assert_eq!(ring.get_weight(boxed_resource.as_ref()), Some(10.0));

        // Try to update non-existent resource
        let nonexistent = MockResource {
            name: "nonexistent".to_string(),
        };
        let boxed_nonexistent: Box<dyn HasId> = Box::new(nonexistent);
        assert!(ring.update_weight(&boxed_nonexistent, 5.0).is_err());
    }

    #[test]
    fn rendezvous_hash_consistency_after_changes() {
        let mut ring = RendezvousRing::new();
        let resources = pick_some_fruit();

        // Add initial resources
        for resource in resources.iter().take(3) {
            ring.add(Box::new(resource.clone()));
        }

        // Map some keys
        let mut initial_mappings = std::collections::HashMap::new();
        for i in 0..50 {
            let key = format!("stable-key-{}", i);
            if let Some(located) = ring.locate(&key) {
                initial_mappings.insert(key, located.id().to_string());
            }
        }

        // Add more resources
        ring.add(Box::new(resources[3].clone()));

        // Check that most keys still map to same resources
        let mut unchanged = 0;
        for (key, original_location) in &initial_mappings {
            if let Some(located) = ring.locate(key)
                && located.id() == original_location
            {
                unchanged += 1;
            }
        }

        // Some keys should remain unchanged, but rendezvous hashing redistributes more than consistent hashing
        // With 4 nodes, adding a 5th should cause about 20% redistribution
        assert!(
            unchanged > initial_mappings.len() / 2,
            "Too much redistribution: {}/{} keys remained unchanged",
            unchanged,
            initial_mappings.len()
        );
    }
}
