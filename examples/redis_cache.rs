//! Distributed Redis cache example comparing ConsistentRing vs RendezvousRing
//!
//! This example demonstrates the differences between consistent hashing and
//! rendezvous hashing for distributing cache keys across Redis instances.
//!
//! Key differences shown:
//! - ConsistentRing: Even distribution, no weights
//! - RendezvousRing: Weighted distribution based on cache capacity
//!
//! Run with: cargo run --example redis_cache

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use lightcycle::{ConsistentRing, HasId, HashRing, RendezvousRing};

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct CacheNode {
    id: String,
    host: String,
    port: u16,
    db: u8,
    capacity_gb: usize, // Cache capacity in GB for weighted distribution
    healthy: Arc<Mutex<bool>>,
    last_health_check: Arc<Mutex<Instant>>,
    connection_count: Arc<Mutex<usize>>,
    // a real implementation would hold onto a redis client as well
}

#[allow(dead_code)]
impl CacheNode {
    fn new(host: impl Into<String>, port: u16, db: u8, capacity_gb: usize) -> Self {
        let host = host.into();
        let id = format!("redis://{}:{}/{} ({}GB)", host, port, db, capacity_gb);

        Self {
            id: id.clone(),
            host,
            port,
            db,
            capacity_gb,
            healthy: Arc::new(Mutex::new(true)),
            last_health_check: Arc::new(Mutex::new(Instant::now())),
            connection_count: Arc::new(Mutex::new(0)),
        }
    }

    fn check_health(&self) -> bool {
        let mut last_check = self
            .last_health_check
            .lock()
            .expect("poisoned mutex on last health check ts");

        if last_check.elapsed() > Duration::from_secs(5) {
            // In a real implementation, you would ping the Redis server here
            // For this example, we'll simulate health checks
            *last_check = Instant::now();

            // Simulate occasional failures (10% chance)
            let is_healthy = rand::random::<f32>() > 0.1;
            *self.healthy.lock().expect("poisoned mutex on the healthy field") = is_healthy;

            if is_healthy {
                println!("✓ {} is healthy", self.id);
            } else {
                println!("✗ {} is unhealthy", self.id);
            }
        }

        *self.healthy.lock().expect("poisoned mutex on the healthy field")
    }

    fn get(&self, key: &str) -> Option<String> {
        if !self.check_health() {
            return None;
        }

        *self
            .connection_count
            .lock()
            .expect("poisoned mutex on connection count") += 1;

        // Simulate cache lookup
        println!("  GET {} from {}", key, self.id);

        // Simulate cache hit/miss (70% hit rate)
        if rand::random::<f32>() < 0.7 {
            Some(format!("cached_value_for_{}", key))
        } else {
            None
        }
    }

    fn set(&self, key: &str, value: &str, ttl: Duration) -> bool {
        if !self.check_health() {
            return false;
        }

        *self
            .connection_count
            .lock()
            .expect("poisoned mutex on connection count") += 1;

        // Simulate cache write
        println!("  SET {} = {} (TTL: {:?}) to {}", key, value, ttl, self.id);
        true
    }

    fn delete(&self, key: &str) -> bool {
        if !self.check_health() {
            return false;
        }

        *self
            .connection_count
            .lock()
            .expect("poisoned mutex on connection count") += 1;

        // Simulate cache delete
        println!("  DEL {} from {}", key, self.id);
        true
    }

    fn stats(&self) -> CacheStats {
        CacheStats {
            id: self.id.clone(),
            healthy: *self.healthy.lock().expect("poisoned mutex on health status field"),
            connection_count: *self
                .connection_count
                .lock()
                .expect("poisoned mutex on connection count"),
        }
    }
}

impl HasId for CacheNode {
    fn id(&self) -> &str {
        &self.id
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct CacheStats {
    id: String,
    healthy: bool,
    connection_count: usize,
}

#[allow(dead_code)]
struct DistributedCache {
    ring: Arc<Mutex<ConsistentRing>>,
    caches: Vec<CacheNode>,
}

#[allow(dead_code)]
impl DistributedCache {
    fn new(caches: Vec<CacheNode>) -> Self {
        let mut ring = ConsistentRing::new_with_replica_count(150);

        for cache in &caches {
            ring.add(Box::new(cache.clone()));
        }

        Self {
            ring: Arc::new(Mutex::new(ring)),
            caches,
        }
    }

    fn get(&self, key: &str) -> Option<String> {
        let ring = self.ring.lock().ok()?;

        // Find which cache should handle this key
        if let Some(cache_id_box) = ring.locate(key) {
            let cache_id = cache_id_box.id();
            // Find the actual cache instance
            for cache in &self.caches {
                if cache.id() == cache_id {
                    return cache.get(key);
                }
            }
        }

        // Try fallback caches if primary is unavailable
        for cache in &self.caches {
            if let Some(value) = cache.get(key) {
                println!("  Found in fallback cache: {}", cache.id);
                return Some(value);
            }
        }

        None
    }

    fn set(&self, key: &str, value: &str, ttl: Duration) -> bool {
        let ring = self.ring.lock().expect("poisoned mutex");

        if let Some(cache_id_box) = ring.locate(key) {
            let cache_id = cache_id_box.id();
            // Find the actual cache instance
            for cache in &self.caches {
                if cache.id() == cache_id {
                    return cache.set(key, value, ttl);
                }
            }
        }
        false
    }

    fn delete(&self, key: &str) -> bool {
        let ring = self.ring.lock().expect("poisoned mutex");

        if let Some(cache_id_box) = ring.locate(key) {
            let cache_id = cache_id_box.id();
            // Find the actual cache instance
            for cache in &self.caches {
                if cache.id() == cache_id {
                    return cache.delete(key);
                }
            }
        }
        false
    }

    fn rebalance(&mut self) {
        println!("\n=== Rebalancing ring based on health ===");
        let mut ring = self.ring.lock().expect("poisoned mutex");

        for cache in &self.caches {
            if !cache.check_health() {
                println!("Removing unhealthy cache: {}", cache.id);
                ring.remove(cache.id()).ok();
            }
        }

        for cache in &self.caches {
            if cache.check_health() && ring.locate(cache.id()).is_none() {
                println!("Re-adding healthy cache: {}", cache.id);
                ring.add(Box::new(cache.clone()));
            }
        }
    }

    fn stats(&self) -> Vec<CacheStats> {
        self.caches.iter().map(|c| c.stats()).collect()
    }
}

// Simulate random number generation (in real code, use the rand crate)
mod rand {
    use std::time::{SystemTime, UNIX_EPOCH};

    #[allow(dead_code)]
    pub fn random<T>() -> T
    where
        T: RandomValue,
    {
        T::random()
    }

    #[allow(dead_code)]
    pub trait RandomValue {
        fn random() -> Self;
    }

    impl RandomValue for f32 {
        fn random() -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("should get system time")
                .subsec_nanos();
            (nanos % 1000) as f32 / 1000.0
        }
    }
}

fn main() {
    println!("=== ConsistentRing vs RendezvousRing Comparison ===\n");

    // Create Redis cache instances with different capacities
    let caches = vec![
        CacheNode::new("localhost", 6379, 0, 1), // 1GB cache
        CacheNode::new("localhost", 6380, 0, 2), // 2GB cache
        CacheNode::new("localhost", 6381, 0, 4), // 4GB cache
        CacheNode::new("localhost", 6382, 0, 8), // 8GB cache
    ];

    demonstrate_consistent_ring(&caches);
    println!();
    demonstrate_rendezvous_ring(&caches);
    println!();
    compare_distributions(&caches);
}

fn demonstrate_consistent_ring(caches: &[CacheNode]) {
    println!("=== ConsistentRing (Traditional Consistent Hashing) ===");

    let mut consistent_ring = ConsistentRing::new_with_replica_count(150);
    for cache in caches {
        consistent_ring.add(Box::new(cache.clone()));
    }

    println!("✓ Even distribution regardless of cache size");
    let mut distribution = HashMap::new();

    for i in 0..1000 {
        let key = format!("key-{}", i);
        if let Some(cache) = consistent_ring.locate(&key) {
            *distribution.entry(cache.id().to_string()).or_insert(0) += 1;
        }
    }

    for (cache_id, count) in &distribution {
        let percentage = (*count as f64 / 10.0).round();
        println!("  {}: {} keys (~{}%)", cache_id, count, percentage);
    }
}

fn demonstrate_rendezvous_ring(caches: &[CacheNode]) {
    println!("=== RendezvousRing (Weighted Rendezvous Hashing) ===");

    let mut rendezvous_ring = RendezvousRing::new();
    for cache in caches {
        // Weight by capacity - bigger caches get more load
        rendezvous_ring.add_weighted(Box::new(cache.clone()), cache.capacity_gb as f64);
    }

    println!("✓ Weighted distribution based on cache capacity");
    let mut distribution = HashMap::new();

    for i in 0..1000 {
        let key = format!("key-{}", i);
        if let Some(cache) = rendezvous_ring.locate(&key) {
            *distribution.entry(cache.id().to_string()).or_insert(0) += 1;
        }
    }

    for (cache_id, count) in &distribution {
        let percentage = (*count as f64 / 10.0).round();
        println!("  {}: {} keys (~{}%)", cache_id, count, percentage);
    }
}

fn compare_distributions(caches: &[CacheNode]) {
    println!("=== Algorithm Comparison ===");

    // Use generic function to show both work with same interface
    let mut consistent_ring = ConsistentRing::new_with_replica_count(100);
    let mut rendezvous_ring = RendezvousRing::new();

    for cache in caches {
        consistent_ring.add(Box::new(cache.clone()));
        rendezvous_ring.add_weighted(Box::new(cache.clone()), cache.capacity_gb as f64);
    }

    println!("Memory usage:");
    println!("  ConsistentRing entries: {} (with replicas)", consistent_ring.len());
    println!("  RendezvousRing entries: {} (no replicas)", rendezvous_ring.len());

    println!("\nCapabilities:");
    println!("  ConsistentRing: ✓ Fast lookup, ✗ No weighting");
    println!("  RendezvousRing: ✓ Weighted nodes, ✓ Memory efficient, ✓ Dynamic weights");

    // Demonstrate that both implement the same HashRing trait
    let test_key = "test:generic:key";
    if let Some(node) = consistent_ring.locate(test_key) {
        println!("  ConsistentRing: '{}' -> {}", test_key, node.id());
    }
    if let Some(node) = rendezvous_ring.locate(test_key) {
        println!("  RendezvousRing: '{}' -> {}", test_key, node.id());
    }
}
