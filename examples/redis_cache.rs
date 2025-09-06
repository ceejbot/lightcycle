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

use anyhow::Result;
use lightcycle::{ConsistentRing, HasId, HashRing, RendezvousRing};
use redis::{Client, Commands, Connection, SetExpiry, SetOptions};

const HEALTH_CHECK_INTERVAL_SEC: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct CacheNode {
    id: String,
    uri: String,
    capacity_gb: usize, // Cache capacity in GB for weighted distribution
    healthy: Arc<Mutex<bool>>,
    last_health_check: Arc<Mutex<Instant>>,
    connection_count: Arc<Mutex<usize>>,
    client: Arc<Mutex<Client>>,
}

#[allow(dead_code)]
impl CacheNode {
    fn new(host: impl Into<String>, port: u16, db: u8, capacity_gb: usize) -> Result<Self> {
        let host = host.into();
        let id = format!("valkey://{host}:{port}/{db} ({capacity_gb}GB)");
        let uri = format!("valkey://{host}:{port}/{db}");
        let client = Client::open(uri.as_str())?;
        Ok(Self {
            id: id.clone(),
            uri,
            capacity_gb,
            healthy: Arc::new(Mutex::new(true)),
            last_health_check: Arc::new(Mutex::new(Instant::now())),
            connection_count: Arc::new(Mutex::new(0)),
            client: Arc::new(Mutex::new(client)),
        })
    }

    fn connection(&self) -> Result<Connection> {
        // perhaps a little optimistic of us
        let guard = match self.client.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        Ok(guard.get_connection()?) // coaxing the type into anyhow::Error
    }

    fn incr_count(&self) {
        let mut count = match self.connection_count.lock() {
            Ok(v) => v,
            Err(poisoned) => poisoned.into_inner(),
        };
        *count += 1;
    }

    fn check_health(&self) -> Result<bool> {
        let mut last_check = self
            .last_health_check
            .lock()
            .expect("poisoned mutex on last health check ts");

        if last_check.elapsed() > HEALTH_CHECK_INTERVAL_SEC {
            let mut conn = self.connection()?;
            let is_healthy = match conn.ping::<String>() {
                Ok(_v) => true,
                Err(_) => false,
            };
            *last_check = Instant::now();
            *self.healthy.lock().expect("poisoned mutex on the healthy field") = is_healthy;
        }

        Ok(*self.healthy.lock().expect("poisoned mutex on the healthy field"))
    }

    fn get(&self, key: &str) -> Option<String> {
        if !self.check_health().ok()? {
            return None;
        }

        self.incr_count();
        let mut conn = self.connection().ok()?;
        let value = conn.get::<&str, String>(key).ok()?;
        Some(value)
    }

    fn set(&self, key: &str, value: &str, ttl: Duration) -> Result<bool> {
        if !self.check_health()? {
            return Ok(false);
        }

        let mut conn = self.connection()?;
        let expiration = SetExpiry::EX(ttl.as_secs());
        let options = SetOptions::default().with_expiration(expiration);
        let was_set = conn.set_options(key, value, options)?;
        Ok(was_set)
    }

    fn delete(&self, key: &str) -> Result<bool> {
        if !self.check_health()? {
            return Ok(false);
        }
        self.incr_count();
        let mut conn = self.connection()?;
        let count = conn.del::<&str, usize>(key)?;
        Ok(count > 0)
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

    fn set(&self, key: &str, value: &str, ttl: Duration) -> Result<bool> {
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
        Ok(false)
    }

    fn delete(&self, key: &str) -> Result<bool> {
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
        Ok(false)
    }

    fn rebalance(&mut self) -> Result<()> {
        println!("\n=== Rebalancing ring based on health ===");
        let mut ring = self.ring.lock().expect("poisoned mutex");

        for cache in &self.caches {
            if !cache.check_health()? {
                println!("Removing unhealthy cache: {}", cache.id);
                ring.remove(cache.id()).ok();
            }
        }

        for cache in &self.caches {
            if cache.check_health()? && ring.locate(cache.id()).is_none() {
                println!("Re-adding healthy cache: {}", cache.id);
                ring.add(Box::new(cache.clone()));
            }
        }
        Ok(())
    }

    fn stats(&self) -> Vec<CacheStats> {
        self.caches.iter().map(|c| c.stats()).collect()
    }
}

fn main() -> Result<()> {
    println!("=== ConsistentRing vs RendezvousRing Comparison ===\n");

    // Create Redis cache instances with different capacities
    let caches = vec![
        CacheNode::new("localhost", 6379, 0, 1)?, // 1GB cache
        CacheNode::new("localhost", 6380, 0, 2)?, // 2GB cache
        CacheNode::new("localhost", 6381, 0, 4)?, // 4GB cache
        CacheNode::new("localhost", 6382, 0, 8)?, // 8GB cache
    ];

    demonstrate_consistent_ring(&caches);
    println!();
    demonstrate_rendezvous_ring(&caches);
    println!();
    compare_distributions(&caches);

    Ok(())
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
