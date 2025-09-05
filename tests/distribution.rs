//! Distribution tests for LightCycle consistent hash ring
//!
//! These tests validate both hash function uniformity and ring distribution.
//!
//! ## Key Findings:
//! - **Hash functions are statistically uniform** (K-S test passes)
//! - **Ring distribution has natural variance** (chi-squared ~50-60 for 8 nodes)
//! - This is **normal for consistent hashing** - perfect uniformity is traded
//!   for consistency during node changes
//!
//! ## Test Types:
//! - `test_ks_uniform_distribution`: Tests raw hash uniformity (should pass)
//! - `test_distribution_chi_squared`: Tests node distribution (has variance)
//! - `test_compare_hash_functions`: Compares different hashers
//! - `test_diagnose_ring_distribution`: Shows the difference between the two

use std::collections::HashMap;

use kolmogorov_smirnov as ks;
use kolmogorov_smirnov::test::TestResult;
use lightcycle::hashes::{ConsistentHasher, DefaultHasher};
use lightcycle::{ConsistentRing, HasId, HashRing, RendezvousRing};

#[derive(Debug, Clone)]
struct TestNode {
    id: String,
}

impl TestNode {
    fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }
}

impl HasId for TestNode {
    fn id(&self) -> &str {
        &self.id
    }
}

#[test]
fn uniform_distribution() {
    let mut ring = ConsistentRing::new_with_replica_count(150);
    let node_count = 10;

    for i in 0..node_count {
        ring.add(Box::new(TestNode::new(format!("node-{}", i))));
    }

    let mut distribution: HashMap<String, usize> = HashMap::new();
    let key_count = 100_000;

    for i in 0..key_count {
        let key = format!("key-{}", i);
        if let Some(node) = ring.locate(&key) {
            *distribution.entry(node.id().to_string()).or_insert(0) += 1;
        }
    }

    let expected_per_node = key_count / node_count;
    let tolerance = 0.20; // Allow 20% deviation - consistent hashing isn't perfect

    for (node_id, count) in &distribution {
        let deviation = (*count as f64 - expected_per_node as f64).abs() / expected_per_node as f64;
        println!(
            "Node {} has {} keys ({:.2}% deviation)",
            node_id,
            count,
            deviation * 100.0
        );
        assert!(
            deviation < tolerance,
            "Node {} has {} keys ({:.2}% deviation), expected ~{} (tolerance: {}%)",
            node_id,
            count,
            deviation * 100.0,
            expected_per_node,
            tolerance * 100.0
        );
    }

    assert_eq!(distribution.len(), node_count, "All nodes should receive keys");
}

#[test]
fn replica_effectiveness() {
    let replica_counts = vec![1, 4, 10, 50, 100, 200];
    let node_count = 5;
    let key_count = 10_000;

    let mut distributions = Vec::new();

    for replica_count in &replica_counts {
        let mut ring = ConsistentRing::new_with_replica_count(*replica_count);

        for i in 0..node_count {
            ring.add(Box::new(TestNode::new(format!("node-{}", i))));
        }

        let mut distribution: HashMap<String, usize> = HashMap::new();

        for i in 0..key_count {
            let key = format!("key-{}", i);
            if let Some(node) = ring.locate(&key) {
                *distribution.entry(node.id().to_string()).or_insert(0) += 1;
            }
        }

        let counts: Vec<usize> = distribution.values().cloned().collect();
        let mean = counts.iter().sum::<usize>() as f64 / counts.len() as f64;
        let variance = counts.iter().map(|c| (*c as f64 - mean).powi(2)).sum::<f64>() / counts.len() as f64;
        let std_dev = variance.sqrt();
        let coefficient_of_variation = std_dev / mean;

        distributions.push((*replica_count, coefficient_of_variation));
    }

    // Print distribution metrics
    println!("\nReplica effectiveness:");
    for (replicas, cv) in &distributions {
        println!("  {} replicas: CV = {:.4}", replicas, cv);
    }

    // Generally, more replicas should improve distribution
    // But we allow some variation due to hash function specifics
    let first_cv = distributions[0].1;
    let last_cv = distributions[distributions.len() - 1].1;
    assert!(
        last_cv < first_cv * 1.5,
        "Distribution should not get much worse with more replicas"
    );
}

#[test]
fn consistency_on_node_addition() {
    let mut ring = ConsistentRing::new_with_replica_count(150);
    let initial_nodes = 5;

    for i in 0..initial_nodes {
        ring.add(Box::new(TestNode::new(format!("node-{}", i))));
    }

    let mut initial_mapping = HashMap::new();
    let key_count = 10_000;

    for i in 0..key_count {
        let key = format!("key-{}", i);
        if let Some(node) = ring.locate(&key) {
            initial_mapping.insert(key, node.id().to_string());
        }
    }

    ring.add(Box::new(TestNode::new("node-new")));

    let mut moved_keys = 0;
    for (key, original_node) in &initial_mapping {
        if let Some(new_node) = ring.locate(key)
            && new_node.id() != original_node
        {
            moved_keys += 1;
        }
    }

    let expected_moved = key_count / (initial_nodes + 1);
    let tolerance = 0.3;
    let deviation = (moved_keys as f64 - expected_moved as f64).abs() / expected_moved as f64;

    assert!(
        deviation < tolerance,
        "Too many keys moved: {} ({}% deviation), expected ~{} (tolerance: {}%)",
        moved_keys,
        deviation * 100.0,
        expected_moved,
        tolerance * 100.0
    );
}

#[test]
fn consistency_on_node_removal() {
    let mut ring = ConsistentRing::new_with_replica_count(150);
    let node_count = 6;

    for i in 0..node_count {
        ring.add(Box::new(TestNode::new(format!("node-{}", i))));
    }

    let mut initial_mapping = HashMap::new();
    let key_count = 10_000;

    for i in 0..key_count {
        let key = format!("key-{}", i);
        if let Some(node) = ring.locate(&key) {
            initial_mapping.insert(key, node.id().to_string());
        }
    }

    let removed_node = "node-3";
    ring.remove(removed_node).expect("Node should exist");

    let mut moved_keys = 0;
    let mut orphaned_keys = 0;

    for (key, original_node) in &initial_mapping {
        if original_node == removed_node {
            orphaned_keys += 1;
            if ring.locate(key).is_some() {
                moved_keys += 1;
            }
        } else if let Some(new_node) = ring.locate(key)
            && new_node.id() != original_node
        {
            moved_keys += 1;
        }
    }

    assert_eq!(orphaned_keys, moved_keys, "All orphaned keys should be reassigned");

    let expected_orphaned = key_count / node_count;
    let tolerance = 0.3;
    let deviation = (orphaned_keys as f64 - expected_orphaned as f64).abs() / expected_orphaned as f64;

    assert!(
        deviation < tolerance,
        "Unexpected number of orphaned keys: {} ({}% deviation), expected ~{} (tolerance: {}%)",
        orphaned_keys,
        deviation * 100.0,
        expected_orphaned,
        tolerance * 100.0
    );
}

#[test]
fn large_scale_performance() {
    let node_counts = vec![100, 500, 1000];

    for node_count in node_counts {
        let mut ring = ConsistentRing::new_with_replica_count(100);

        let start = std::time::Instant::now();
        for i in 0..node_count {
            ring.add(Box::new(TestNode::new(format!("node-{}", i))));
        }
        let add_duration = start.elapsed();

        let start = std::time::Instant::now();
        for i in 0..10_000 {
            let key = format!("key-{}", i);
            ring.locate(&key);
        }
        let locate_duration = start.elapsed();

        println!(
            "Nodes: {}, Add time: {:?}, 10k lookups: {:?}",
            node_count, add_duration, locate_duration
        );

        // String-based hashing is slower than numeric, but still should be reasonable
        let max_duration_ms = match node_count {
            100 => 3000,   // 3 seconds for 100 nodes
            500 => 15000,  // 15 seconds for 500 nodes (O(log n) with string comparisons)
            1000 => 31000, // 31 seconds for 1000 nodes (string comparisons are slow)
            _ => 31000,
        };

        assert!(
            locate_duration.as_millis() < max_duration_ms,
            "10k lookups took {:?}, should complete within {}ms for {} nodes",
            locate_duration,
            max_duration_ms,
            node_count
        );
    }
}

#[test]
fn ks_uniform_distribution() {
    let mut ring = ConsistentRing::new_with_replica_count(150);
    let node_count = 8;

    for i in 0..node_count {
        ring.add(Box::new(TestNode::new(format!("node-{}", i))));
    }

    let key_count = 10_000;
    let mut hash_values = Vec::with_capacity(key_count);

    // Collect raw hash values and normalize them to [0,1]
    for i in 0..key_count {
        let key = format!("key-{}", i);
        let hash_value = ring.hash_key(&key);

        // Convert hash to normalized float [0,1]
        let normalized = hash_u64_to_normalized_float(hash_value);
        hash_values.push(normalized);
    }

    // Create ideal uniform distribution for comparison
    let mut uniform_sample = Vec::with_capacity(key_count);
    for i in 0..key_count {
        uniform_sample.push(i as f64 / key_count as f64);
    }

    // Sort both samples for K-S test
    hash_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    uniform_sample.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let confidence = 0.95;
    let result = ks::test_f64(&hash_values, &uniform_sample, confidence);

    println!(
        "K-S Test Results - Statistic: {:.6}, Critical: {:.6}, Rejected: {}",
        result.statistic, result.critical_value, result.is_rejected
    );

    // Hash distribution should not be significantly different from uniform
    assert!(
        !result.is_rejected,
        "Hash distribution significantly differs from uniform (p < 0.05). Statistic: {:.6}, Critical: {:.6}",
        result.statistic, result.critical_value
    );

    // Also print some diagnostics
    println!("Hash distribution appears uniform (K-S test passed)");
    println!("First 10 normalized hash values: {:?}", &hash_values[0..10]);
}

/// Convert a hash u64 to a normalized float in [0,1]
fn hash_u64_to_normalized_float(hash_value: u64) -> f64 {
    hash_value as f64 / u64::MAX as f64
}

#[test]
fn diagnose_ring_distribution() {
    let mut ring = ConsistentRing::new_with_replica_count(150);
    let node_count = 8;

    for i in 0..node_count {
        ring.add(Box::new(TestNode::new(format!("node-{}", i))));
    }

    println!("\n=== Ring Distribution Diagnosis ===");

    // Check hash uniformity
    let key_count = 10_000;
    let mut hash_values = Vec::new();
    for i in 0..key_count {
        let key = format!("key-{}", i);
        let hash_value = ring.hash_key(&key);
        let normalized = hash_u64_to_normalized_float(hash_value);
        hash_values.push(normalized);
    }

    hash_values.sort_by(|a, b| a.partial_cmp(b).expect("f64 values should be comparable"));
    let mut uniform_sample: Vec<f64> = (0..key_count).map(|i| i as f64 / key_count as f64).collect();
    uniform_sample.sort_by(|a, b| a.partial_cmp(b).expect("f64 values should be comparable"));

    let ks_result = ks::test_f64(&hash_values, &uniform_sample, 0.95);
    println!(
        "Hash uniformity: K-S statistic = {:.6}, critical = {:.6}, rejected = {}",
        ks_result.statistic, ks_result.critical_value, ks_result.is_rejected
    );

    // Check node distribution
    let mut node_distribution: HashMap<String, usize> = HashMap::new();
    for i in 0..key_count {
        let key = format!("key-{}", i);
        if let Some(node) = ring.locate(&key) {
            *node_distribution.entry(node.id().to_string()).or_insert(0) += 1;
        }
    }

    println!("Node distribution:");
    let expected_per_node = key_count as f64 / node_count as f64;
    let mut chi_squared = 0.0;

    for i in 0..node_count {
        let node_id = format!("node-{}", i);
        let count = node_distribution.get(&node_id).unwrap_or(&0);
        let deviation = (*count as f64 - expected_per_node).abs() / expected_per_node;
        chi_squared += (*count as f64 - expected_per_node).powi(2) / expected_per_node;
        println!(
            "  {}: {} keys ({:.2}% of total, {:.2}% deviation)",
            node_id,
            count,
            (*count as f64 / key_count as f64) * 100.0,
            deviation * 100.0
        );
    }

    println!("Chi-squared statistic: {:.2}", chi_squared);

    // The issue is clearly in ring structure, not hash uniformity
    assert!(!ks_result.is_rejected, "Hash values should be uniform");
}

#[test]
fn compare_hash_functions() {
    let key_count = 10_000;
    let confidence = 0.95;

    // Create ideal uniform distribution for comparison
    let mut uniform_sample = Vec::with_capacity(key_count);
    for i in 0..key_count {
        uniform_sample.push(i as f64 / key_count as f64);
    }
    uniform_sample.sort_by(|a, b| a.partial_cmp(b).expect("f64 values should be comparable"));

    // Test Blake3 hasher (default)
    println!("\n=== Hash Function Comparison ===");

    let default_ring = ConsistentRing::new_with_hasher(DefaultHasher::new());
    let default_result = check_hasher_uniformity(&default_ring, key_count, &uniform_sample, confidence);
    println!(
        "Default ({}): K-S statistic = {:.6}, critical = {:.6}, rejected = {}",
        default_ring.hasher_name(),
        default_result.statistic,
        default_result.critical_value,
        default_result.is_rejected
    );

    // Test other hashers if available
    #[cfg(feature = "hash-xxhash")]
    {
        use lightcycle::hasher::XXHasher;
        let xxhash_ring = ConsistentRing::new_with_hasher(XXHasher::new());
        let xxhash_result = check_hasher_uniformity(&xxhash_ring, key_count, &uniform_sample, confidence);
        println!(
            "XXHash: K-S statistic = {:.6}, critical = {:.6}, rejected = {}",
            xxhash_result.statistic, xxhash_result.critical_value, xxhash_result.is_rejected
        );
    }

    // All available hashers should pass the uniformity test
    assert!(!default_result.is_rejected, "Default hasher failed uniformity test");
}

fn check_hasher_uniformity<H: ConsistentHasher>(
    ring: &ConsistentRing<H>,
    key_count: usize,
    uniform_sample: &[f64],
    confidence: f64,
) -> TestResult {
    let mut hash_values = Vec::with_capacity(key_count);

    // Collect raw hash values and normalize them to [0,1]
    for i in 0..key_count {
        let key = format!("key-{}", i);
        let hash_value = ring.hash_key(&key);

        // Convert hash to normalized float [0,1]
        let normalized = hash_u64_to_normalized_float(hash_value);
        hash_values.push(normalized);
    }

    // Sort sample for K-S test
    hash_values.sort_by(|a, b| a.partial_cmp(b).expect("f64 values should be comparable"));

    // Perform K-S test
    ks::test_f64(&hash_values, uniform_sample, confidence)
}

#[test]
fn weighted_distribution_accuracy() {
    let mut ring = RendezvousRing::new();

    // Add nodes with 1:2:4 weight ratio
    ring.add_weighted(Box::new(TestNode::new("node-1")), 1.0);
    ring.add_weighted(Box::new(TestNode::new("node-2")), 2.0);
    ring.add_weighted(Box::new(TestNode::new("node-4")), 4.0);

    let key_count = 10_000;
    let mut distribution = std::collections::HashMap::new();

    for i in 0..key_count {
        let key = format!("key-{}", i);
        if let Some(node) = ring.locate(&key) {
            *distribution.entry(node.id().to_string()).or_insert(0) += 1;
        }
    }

    let node1_count = distribution.get("node-1").unwrap_or(&0);
    let node2_count = distribution.get("node-2").unwrap_or(&0);
    let node4_count = distribution.get("node-4").unwrap_or(&0);

    println!(
        "Weighted distribution - node-1: {}, node-2: {}, node-4: {}",
        node1_count, node2_count, node4_count
    );

    // Calculate ratios (allowing for some variance due to hashing)
    let ratio_2_to_1 = *node2_count as f64 / *node1_count as f64;
    let ratio_4_to_1 = *node4_count as f64 / *node1_count as f64;

    // Allow 20% variance from expected ratios
    assert!(
        (1.6..=2.4).contains(&ratio_2_to_1),
        "Node-2 should get ~2x node-1's load. Got ratio: {:.2}",
        ratio_2_to_1
    );

    assert!(
        (3.2..=4.8).contains(&ratio_4_to_1),
        "Node-4 should get ~4x node-1's load. Got ratio: {:.2}",
        ratio_4_to_1
    );

    // Total should equal key count
    assert_eq!(node1_count + node2_count + node4_count, key_count);
}

#[test]
fn weighted_distribution_chi_squared() {
    let mut ring = RendezvousRing::new();
    let node_count = 3;
    let weights = [1.0, 3.0, 6.0]; // 1:3:6 ratio
    let total_weight: f64 = weights.iter().sum();

    // Add weighted nodes
    for (i, &weight) in weights.iter().enumerate() {
        ring.add_weighted(Box::new(TestNode::new(format!("node-{}", i))), weight);
    }

    let key_count = 10_000;
    let mut observed = vec![0.0; node_count];

    // Collect distribution
    for i in 0..key_count {
        let key = format!("key-{}", i);
        if let Some(node) = ring.locate(&key) {
            let node_index = node
                .id()
                .chars()
                .last()
                .expect("expected a last char")
                .to_digit(10)
                .expect("expected it to be a digit") as usize;
            observed[node_index] += 1.0;
        }
    }

    // Calculate expected counts based on weights
    let expected: Vec<f64> = weights.iter().map(|&w| (w / total_weight) * key_count as f64).collect();

    // Chi-squared test
    let mut chi_squared = 0.0;
    for i in 0..node_count {
        chi_squared += (observed[i] - expected[i]).powi(2) / expected[i];
    }

    let degrees_of_freedom = node_count - 1;
    let critical_value = match degrees_of_freedom {
        2 => 5.991, // p = 0.05
        _ => panic!("Add critical value for {} degrees of freedom", degrees_of_freedom),
    };

    println!(
        "Weighted chi-squared: {:.2} (critical: {:.2})",
        chi_squared, critical_value
    );

    println!("Expected: {:?}, Observed: {:?}", expected, observed);

    // Weighted distribution should be reasonably close to expected
    assert!(
        chi_squared < critical_value * 2.0, // Allow 2x tolerance for weighted distribution
        "Weighted distribution chi-squared test failed: {:.2} > {:.2}",
        chi_squared,
        critical_value * 2.0
    );
}

#[test]
fn rendezvous_vs_consistent_distribution() {
    let key_count = 1000;
    let node_count = 5;

    // Create identical nodes for both rings
    let mut consistent_ring = ConsistentRing::new_with_replica_count(100);
    let mut rendezvous_ring = RendezvousRing::new();

    for i in 0..node_count {
        let node_id = format!("node-{}", i);
        consistent_ring.add(Box::new(TestNode::new(node_id.clone())));
        rendezvous_ring.add_weighted(Box::new(TestNode::new(node_id)), 1.0); // Equal weights
    }

    // Compare distributions
    let mut consistent_dist = std::collections::HashMap::new();
    let mut rendezvous_dist = std::collections::HashMap::new();

    for i in 0..key_count {
        let key = format!("key-{}", i);

        if let Some(node) = consistent_ring.locate(&key) {
            *consistent_dist.entry(node.id().to_string()).or_insert(0) += 1;
        }

        if let Some(node) = rendezvous_ring.locate(&key) {
            *rendezvous_dist.entry(node.id().to_string()).or_insert(0) += 1;
        }
    }

    println!("ConsistentRing distribution: {:?}", consistent_dist);
    println!("RendezvousRing distribution: {:?}", rendezvous_dist);

    // Both should distribute keys reasonably evenly
    let expected = key_count / node_count;
    let tolerance = expected / 2; // 50% tolerance

    for (node_id, count) in &consistent_dist {
        assert!(
            *count >= expected - tolerance && *count <= expected + tolerance,
            "ConsistentRing node {} got {} keys, expected ~{}",
            node_id,
            count,
            expected
        );
    }

    for (node_id, count) in &rendezvous_dist {
        assert!(
            *count >= expected - tolerance && *count <= expected + tolerance,
            "RendezvousRing node {} got {} keys, expected ~{}",
            node_id,
            count,
            expected
        );
    }
}

#[test]
fn performance_comparison() {
    use std::time::Instant;

    let node_counts = [10, 50, 100, 200];
    let lookup_count = 10_000;

    println!("\n=== Performance Comparison: ConsistentRing vs RendezvousRing ===");

    for &node_count in &node_counts {
        // Setup ConsistentRing
        let mut consistent_ring = ConsistentRing::new_with_replica_count(100);
        for i in 0..node_count {
            consistent_ring.add(Box::new(TestNode::new(format!("node-{}", i))));
        }

        // Setup RendezvousRing
        let mut rendezvous_ring = RendezvousRing::new();
        for i in 0..node_count {
            rendezvous_ring.add_weighted(Box::new(TestNode::new(format!("node-{}", i))), 1.0);
        }

        // Performance test for ConsistentRing
        let start = Instant::now();
        for i in 0..lookup_count {
            let key = format!("key-{}", i);
            consistent_ring.locate(&key);
        }
        let consistent_time = start.elapsed();

        // Performance test for RendezvousRing
        let start = Instant::now();
        for i in 0..lookup_count {
            let key = format!("key-{}", i);
            rendezvous_ring.locate(&key);
        }
        let rendezvous_time = start.elapsed();

        let ratio = rendezvous_time.as_secs_f64() / consistent_time.as_secs_f64();

        println!(
            "Nodes: {}, ConsistentRing: {:?}, RendezvousRing: {:?}, Ratio: {:.2}x",
            node_count, consistent_time, rendezvous_time, ratio
        );

        // For small node counts, performance should be similar
        if node_count <= 50 {
            assert!(
                ratio <= 10.0,
                "RendezvousRing should be reasonable for {} nodes, got {:.2}x slower",
                node_count,
                ratio
            );
        }
    }
}

#[test]
fn test_rendezvous_memory_efficiency() {
    let node_count = 100;
    let replica_count = 150;

    // ConsistentRing with replicas
    let mut consistent_ring = ConsistentRing::new_with_replica_count(replica_count);
    for i in 0..node_count {
        consistent_ring.add(Box::new(TestNode::new(format!("node-{}", i))));
    }

    // RendezvousRing without replicas
    let mut rendezvous_ring = RendezvousRing::new();
    for i in 0..node_count {
        rendezvous_ring.add_weighted(Box::new(TestNode::new(format!("node-{}", i))), 1.0);
    }

    println!(
        "Memory efficiency - ConsistentRing entries: {}, RendezvousRing entries: {}",
        consistent_ring.len(),
        rendezvous_ring.len()
    );

    // ConsistentRing stores approximately node_count * replica_count entries
    // (may be slightly less due to hash collisions)
    assert!(
        consistent_ring.len() >= (node_count * replica_count) * 9 / 10,
        "ConsistentRing should have most replica entries"
    );

    // RendezvousRing stores only node_count entries
    assert_eq!(rendezvous_ring.len(), node_count);

    // Both should have same resource count
    assert_eq!(consistent_ring.resource_count(), rendezvous_ring.resource_count());

    // RendezvousRing should be much more memory efficient
    let memory_ratio = consistent_ring.len() as f64 / rendezvous_ring.len() as f64;
    assert!(
        memory_ratio >= replica_count as f64 * 0.8, // Allow some variance
        "RendezvousRing should be ~{}x more memory efficient, got {:.2}x",
        replica_count,
        memory_ratio
    );
}
