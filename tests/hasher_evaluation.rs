//! Comprehensive evaluation of hash functions for rendezvous hashing
//!
//! This test evaluates all available hash functions across multiple criteria:
//! - Distribution quality (statistical uniformity)
//! - Performance (speed)
//! - Consistency and collision resistance
//! - Suitability for rendezvous hashing

use lightcycle::hasher::ConsistentHasher;
use lightcycle::{HashRing, HasId, RendezvousRing};
use std::collections::HashMap;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
struct TestNode {
    id: String,
}

impl HasId for TestNode {
    fn id(&self) -> &str {
        &self.id
    }
}

/// Test result for a single hasher
#[derive(Debug)]
struct HasherEvaluation {
    name: String,
    hash_speed_ns: f64,
    distribution_chi_squared: f64,
    distribution_stddev: f64,
    weighted_accuracy: f64,
    consistency_score: f64,
}

/// Measure hash speed in nanoseconds per operation
fn measure_hash_speed<H: ConsistentHasher>(hasher: &H, iterations: usize) -> f64 {
    let test_keys: Vec<String> = (0..1000).map(|i| format!("test-key-{}", i)).collect();
    
    let start = Instant::now();
    for _ in 0..iterations {
        for key in &test_keys {
            let _ = hasher.hash(key.as_bytes());
        }
    }
    let duration = start.elapsed();
    
    duration.as_nanos() as f64 / (iterations * test_keys.len()) as f64
}

/// Test distribution quality using chi-squared test
fn test_distribution_quality<H: ConsistentHasher>(hasher: H, nodes: usize, keys: usize) -> (f64, f64) {
    let mut ring = RendezvousRing::new_with_hasher(hasher);
    
    // Add nodes
    for i in 0..nodes {
        ring.add(Box::new(TestNode { id: format!("node-{}", i) }));
    }
    
    // Map keys and count distribution
    let mut counts: HashMap<String, usize> = HashMap::new();
    for i in 0..keys {
        let key = format!("key-{}", i);
        if let Some(node) = ring.locate(&key) {
            *counts.entry(node.id().to_string()).or_insert(0) += 1;
        }
    }
    
    // Calculate chi-squared statistic
    let expected = keys as f64 / nodes as f64;
    let mut chi_squared = 0.0;
    let mut sum_sq_diff = 0.0;
    
    for i in 0..nodes {
        let node_id = format!("node-{}", i);
        let observed = counts.get(&node_id).copied().unwrap_or(0) as f64;
        let diff = observed - expected;
        chi_squared += (diff * diff) / expected;
        sum_sq_diff += diff * diff;
    }
    
    let stddev = (sum_sq_diff / nodes as f64).sqrt();
    
    (chi_squared, stddev)
}

/// Test weighted distribution accuracy
fn test_weighted_distribution<H: ConsistentHasher>(hasher: H) -> f64 {
    let mut ring = RendezvousRing::new_with_hasher(hasher);
    
    // Add nodes with different weights
    ring.add_weighted(Box::new(TestNode { id: "light".to_string() }), 1.0);
    ring.add_weighted(Box::new(TestNode { id: "medium".to_string() }), 5.0);
    ring.add_weighted(Box::new(TestNode { id: "heavy".to_string() }), 10.0);
    
    // Map many keys
    let mut counts: HashMap<String, usize> = HashMap::new();
    for i in 0..10000 {
        let key = format!("weighted-key-{}", i);
        if let Some(node) = ring.locate(&key) {
            *counts.entry(node.id().to_string()).or_insert(0) += 1;
        }
    }
    
    // Calculate how close we are to expected ratios
    let light_count = counts.get("light").copied().unwrap_or(0) as f64;
    let medium_count = counts.get("medium").copied().unwrap_or(0) as f64;
    let heavy_count = counts.get("heavy").copied().unwrap_or(0) as f64;
    
    let total_weight = 16.0; // 1 + 5 + 10
    let expected_light = 10000.0 / total_weight;
    let expected_medium = 5.0 * 10000.0 / total_weight;
    let expected_heavy = 10.0 * 10000.0 / total_weight;
    
    // Calculate percentage error from expected
    let light_error = ((light_count - expected_light).abs() / expected_light) * 100.0;
    let medium_error = ((medium_count - expected_medium).abs() / expected_medium) * 100.0;
    let heavy_error = ((heavy_count - expected_heavy).abs() / expected_heavy) * 100.0;
    
    // Return average accuracy (100% - average error)
    100.0 - (light_error + medium_error + heavy_error) / 3.0
}

/// Test consistency - same input always produces same output
fn test_consistency<H: ConsistentHasher>(hasher: H) -> f64 {
    let mut ring = RendezvousRing::new_with_hasher(hasher);
    
    // Add nodes
    for i in 0..10 {
        ring.add(Box::new(TestNode { id: format!("node-{}", i) }));
    }
    
    // Test consistency
    let mut consistency_count = 0;
    let test_count = 1000;
    
    for i in 0..test_count {
        let key = format!("consistency-key-{}", i);
        let first_location = ring.locate(&key).map(|n| n.id().to_string());
        
        // Check 10 times that we get the same result
        let mut consistent = true;
        for _ in 0..10 {
            let location = ring.locate(&key).map(|n| n.id().to_string());
            if location != first_location {
                consistent = false;
                break;
            }
        }
        
        if consistent {
            consistency_count += 1;
        }
    }
    
    (consistency_count as f64 / test_count as f64) * 100.0
}

/// Evaluate a hasher across all criteria
fn evaluate_hasher<H: ConsistentHasher>(hasher: H) -> HasherEvaluation {
    let name = hasher.name().to_string();
    println!("\nEvaluating {}...", name);
    
    // Speed test
    let hash_speed_ns = measure_hash_speed(&hasher, 100);
    println!("  Hash speed: {:.2} ns/op", hash_speed_ns);
    
    // Distribution quality
    let (chi_squared, stddev) = test_distribution_quality(hasher.clone(), 10, 10000);
    println!("  Distribution χ²: {:.2}, σ: {:.2}", chi_squared, stddev);
    
    // Weighted distribution
    let weighted_accuracy = test_weighted_distribution(hasher.clone());
    println!("  Weighted accuracy: {:.1}%", weighted_accuracy);
    
    // Consistency
    let consistency_score = test_consistency(hasher);
    println!("  Consistency: {:.1}%", consistency_score);
    
    HasherEvaluation {
        name,
        hash_speed_ns,
        distribution_chi_squared: chi_squared,
        distribution_stddev: stddev,
        weighted_accuracy,
        consistency_score,
    }
}

#[test]
fn evaluate_all_hashers() {
    println!("\n=== Hash Function Evaluation for Rendezvous Hashing ===");
    
    let mut results = Vec::new();
    
    // Test each available hasher
    #[cfg(feature = "hash-blake3")]
    {
        use lightcycle::hasher::Blake3Hasher;
        results.push(evaluate_hasher(Blake3Hasher::new()));
    }
    
    #[cfg(feature = "hash-xxhash")]
    {
        use lightcycle::hasher::XXHasher;
        results.push(evaluate_hasher(XXHasher::new()));
    }
    
    #[cfg(feature = "hash-metrohash")]
    {
        use lightcycle::hasher::MetroHasher;
        results.push(evaluate_hasher(MetroHasher::new()));
    }
    
    #[cfg(feature = "hash-rapidhash")]
    {
        use lightcycle::hasher::{RapidHashQualityHasher, RapidHashFastHasher};
        results.push(evaluate_hasher(RapidHashQualityHasher::new()));
        results.push(evaluate_hasher(RapidHashFastHasher::new()));
    }
    
    
    #[cfg(feature = "hash-murmur3")]
    {
        use lightcycle::hasher::Murmur3Hasher;
        results.push(evaluate_hasher(Murmur3Hasher::new()));
    }
    
    #[cfg(feature = "hash-blake2")]
    {
        use lightcycle::hasher::Blake2Hasher;
        results.push(evaluate_hasher(Blake2Hasher::new()));
    }
    
    #[cfg(feature = "hash-sha2")]
    {
        use lightcycle::hasher::Sha256Hasher;
        results.push(evaluate_hasher(Sha256Hasher::new()));
    }
    
    #[cfg(feature = "hash-fnv")]
    {
        use lightcycle::hasher::FnvHasher;
        results.push(evaluate_hasher(FnvHasher::new()));
    }
    
    // Print summary
    println!("\n=== Summary Results ===");
    println!("{:<12} | {:>10} | {:>10} | {:>10} | {:>12} | {:>12}",
        "Hasher", "Speed(ns)", "χ²", "σ", "Weighted%", "Consistent%");
    println!("{:-<12}-+-{:-<10}-+-{:-<10}-+-{:-<10}-+-{:-<12}-+-{:-<12}",
        "", "", "", "", "", "");
    
    for result in &results {
        println!("{:<12} | {:>10.2} | {:>10.2} | {:>10.2} | {:>12.1} | {:>12.1}",
            result.name,
            result.hash_speed_ns,
            result.distribution_chi_squared,
            result.distribution_stddev,
            result.weighted_accuracy,
            result.consistency_score
        );
    }
    
    // Find best performers
    println!("\n=== Analysis ===");
    
    let fastest = results.iter().min_by(|a, b| {
        a.hash_speed_ns.partial_cmp(&b.hash_speed_ns).unwrap()
    }).unwrap();
    println!("Fastest hasher: {} ({:.2} ns/op)", fastest.name, fastest.hash_speed_ns);
    
    let best_distribution = results.iter().min_by(|a, b| {
        a.distribution_chi_squared.partial_cmp(&b.distribution_chi_squared).unwrap()
    }).unwrap();
    println!("Best distribution: {} (χ² = {:.2})", best_distribution.name, best_distribution.distribution_chi_squared);
    
    let best_weighted = results.iter().max_by(|a, b| {
        a.weighted_accuracy.partial_cmp(&b.weighted_accuracy).unwrap()
    }).unwrap();
    println!("Best weighted accuracy: {} ({:.1}%)", best_weighted.name, best_weighted.weighted_accuracy);
    
    // Overall recommendation
    println!("\n=== Recommendation ===");
    
    // Score each hasher (lower is better)
    let mut scores: Vec<(String, f64)> = results.iter().map(|r| {
        let speed_score = r.hash_speed_ns / fastest.hash_speed_ns; // Normalized to fastest
        let distribution_score = r.distribution_chi_squared / best_distribution.distribution_chi_squared;
        let weighted_score = best_weighted.weighted_accuracy / r.weighted_accuracy;
        let consistency_penalty = if r.consistency_score < 100.0 { 10.0 } else { 1.0 };
        
        let total_score = speed_score + distribution_score + weighted_score * consistency_penalty;
        (r.name.clone(), total_score)
    }).collect();
    
    scores.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    
    println!("Top 3 recommended hashers for rendezvous hashing:");
    for (i, (name, score)) in scores.iter().take(3).enumerate() {
        println!("  {}. {} (score: {:.2})", i + 1, name, score);
    }
    
    // All hashers should have perfect consistency
    for result in &results {
        assert_eq!(result.consistency_score, 100.0, 
            "{} hasher should have perfect consistency", result.name);
    }
}

#[test]
fn verify_hasher_compatibility() {
    // Verify that all hashers work correctly with RendezvousRing
    
    #[cfg(feature = "hash-blake3")]
    {
        use lightcycle::hasher::Blake3Hasher;
        let mut ring = RendezvousRing::new_with_hasher(Blake3Hasher::new());
        ring.add(Box::new(TestNode { id: "test".to_string() }));
        assert!(ring.locate("key").is_some());
    }
    
    #[cfg(feature = "hash-rapidhash")]
    {
        use lightcycle::hasher::{RapidHashQualityHasher, RapidHashFastHasher};
        
        let mut ring1 = RendezvousRing::new_with_hasher(RapidHashQualityHasher::new());
        ring1.add(Box::new(TestNode { id: "test".to_string() }));
        assert!(ring1.locate("key").is_some());
        
        let mut ring2 = RendezvousRing::new_with_hasher(RapidHashFastHasher::new());
        ring2.add(Box::new(TestNode { id: "test".to_string() }));
        assert!(ring2.locate("key").is_some());
    }
    
    #[cfg(feature = "hash-xxhash")]
    {
        use lightcycle::hasher::XXHasher;
        let mut ring = RendezvousRing::new_with_hasher(XXHasher::new());
        ring.add(Box::new(TestNode { id: "test".to_string() }));
        assert!(ring.locate("key").is_some());
    }
    
    #[cfg(feature = "hash-metrohash")]
    {
        use lightcycle::hasher::MetroHasher;
        let mut ring = RendezvousRing::new_with_hasher(MetroHasher::new());
        ring.add(Box::new(TestNode { id: "test".to_string() }));
        assert!(ring.locate("key").is_some());
    }
    
}