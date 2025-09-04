use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::thread;

use lightcycle::{ConsistentRing, HasId, HashRing};

#[derive(Debug, Clone)]
struct ServerNode {
    id: String,
    healthy: Arc<Mutex<bool>>,
}

impl ServerNode {
    fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            healthy: Arc::new(Mutex::new(true)),
        }
    }

    fn set_health(&self, healthy: bool) {
        *self.healthy.lock().expect("poisoned mutex") = healthy;
    }

    fn _is_healthy(&self) -> bool {
        *self.healthy.lock().expect("poisoned mutex")
    }
}

impl HasId for ServerNode {
    fn id(&self) -> &str {
        &self.id
    }
}

#[test]
fn test_node_failure_recovery() {
    let mut ring = ConsistentRing::new_with_replica_count(100);
    let nodes: Vec<ServerNode> = (0..5).map(|i| ServerNode::new(format!("server-{}", i))).collect();

    for node in &nodes {
        ring.add(Box::new(node.clone()));
    }

    let mut mappings_before = HashMap::new();
    for i in 0..1000 {
        let key = format!("session-{}", i);
        if let Some(node) = ring.locate(&key) {
            mappings_before.insert(key, node.id().to_string());
        }
    }

    nodes[2].set_health(false);
    ring.remove("server-2").expect("server remove should succeed");

    let mut mappings_after = HashMap::new();
    let mut remapped_count = 0;

    for (key, original_node) in &mappings_before {
        if let Some(new_node) = ring.locate(key) {
            let new_id = new_node.id().to_string();
            mappings_after.insert(key.clone(), new_id.clone());

            if original_node == "server-2" || &new_id != original_node {
                remapped_count += 1;
            }
        }
    }

    let affected_keys = mappings_before.iter().filter(|(_, node)| *node == "server-2").count();

    assert!(
        remapped_count >= affected_keys,
        "All keys from failed node should be remapped"
    );

    nodes[2].set_health(true);
    ring.add(Box::new(nodes[2].clone()));

    let mut restored_count = 0;
    for (key, original_node) in &mappings_before {
        if let Some(restored_node) = ring.locate(key)
            && restored_node.id() == original_node
        {
            restored_count += 1;
        }
    }

    assert!(
        restored_count as f64 / mappings_before.len() as f64 > 0.6,
        "Most keys should return to original nodes after recovery"
    );
}

#[test]
fn test_concurrent_access() {
    let ring = Arc::new(Mutex::new(ConsistentRing::new_with_replica_count(150)));

    for i in 0..10 {
        let node = ServerNode::new(format!("node-{}", i));
        ring.lock().expect("poisoned mutex").add(Box::new(node));
    }

    let mut handles = vec![];
    let iterations = 100;
    let thread_count = 8;

    for thread_id in 0..thread_count {
        let ring_clone = Arc::clone(&ring);

        let handle = thread::spawn(move || {
            let mut results = HashMap::new();

            for i in 0..iterations {
                let key = format!("thread-{}-key-{}", thread_id, i);
                let node_id = ring_clone
                    .lock()
                    .expect("poisoned mutex")
                    .locate(&key)
                    .map(|n| n.id().to_string());

                if let Some(id) = node_id {
                    results.insert(key, id);
                }
            }

            results
        });

        handles.push(handle);
    }

    let mut all_results = HashMap::new();
    for handle in handles {
        let thread_results = handle.join().expect("thread join should succeed");
        all_results.extend(thread_results);
    }

    assert_eq!(
        all_results.len(),
        thread_count * iterations,
        "All keys should be processed"
    );

    for thread_id in 0..thread_count {
        for i in 0..iterations {
            let key = format!("thread-{}-key-{}", thread_id, i);
            let first_result = all_results
                .get(&key)
                .expect("should find at least one hit for this key");

            let current = ring
                .lock()
                .expect("poisoned mutex")
                .locate(&key)
                .map(|n| n.id().to_string())
                .expect("we expect to find this value");

            assert_eq!(first_result, &current, "Consistent hashing should produce same results");
        }
    }
}

#[test]
fn test_gradual_scaling() {
    let mut ring = ConsistentRing::new_with_replica_count(100);
    let mut nodes = Vec::new();
    let mut key_mappings: HashMap<String, Vec<String>> = HashMap::new();

    for scaling_step in 0..5 {
        for i in 0..2 {
            let node_id = format!("node-{}-{}", scaling_step, i);
            let node = ServerNode::new(node_id.clone());
            nodes.push(node.clone());
            ring.add(Box::new(node));
        }

        for i in 0..1000 {
            let key = format!("key-{}", i);
            if let Some(node) = ring.locate(&key) {
                key_mappings.entry(key).or_default().push(node.id().to_string());
            }
        }
    }

    for (key, history) in &key_mappings {
        let _unique_nodes: HashSet<_> = history.iter().collect();
        let migration_count = history.windows(2).filter(|w| w[0] != w[1]).count();

        assert!(
            migration_count <= 4,
            "Key {} migrated too many times: {} (history: {:?})",
            key,
            migration_count,
            history
        );
    }
}

#[test]
fn test_ring_merge_split() {
    let mut ring1 = ConsistentRing::new_with_replica_count(100);
    let mut ring2 = ConsistentRing::new_with_replica_count(100);

    for i in 0..5 {
        ring1.add(Box::new(ServerNode::new(format!("dc1-node-{}", i))));
    }

    for i in 0..5 {
        ring2.add(Box::new(ServerNode::new(format!("dc2-node-{}", i))));
    }

    let mut merged_ring = ConsistentRing::new_with_replica_count(100);
    for i in 0..5 {
        merged_ring.add(Box::new(ServerNode::new(format!("dc1-node-{}", i))));
        merged_ring.add(Box::new(ServerNode::new(format!("dc2-node-{}", i))));
    }

    let mut dc1_keys = 0;
    let mut dc2_keys = 0;

    for i in 0..10000 {
        let key = format!("key-{}", i);
        if let Some(node) = merged_ring.locate(&key) {
            if node.id().starts_with("dc1") {
                dc1_keys += 1;
            } else {
                dc2_keys += 1;
            }
        }
    }

    let ratio = dc1_keys as f64 / dc2_keys as f64;
    assert!(
        (0.8..=1.2).contains(&ratio),
        "Keys should be roughly balanced between datacenters: dc1={}, dc2={}, ratio={}",
        dc1_keys,
        dc2_keys,
        ratio
    );
}

#[test]
fn test_hot_key_distribution() {
    let mut ring = ConsistentRing::new_with_replica_count(150);
    let node_count = 10;

    for i in 0..node_count {
        ring.add(Box::new(ServerNode::new(format!("cache-{}", i))));
    }

    let hot_keys = vec![
        "user:123:profile",
        "trending:today",
        "config:global",
        "session:abc123",
        "popular:item:456",
    ];

    let mut hot_key_distribution = HashMap::new();

    for _ in 0..1000 {
        for key in &hot_keys {
            if let Some(node) = ring.locate(key) {
                *hot_key_distribution
                    .entry((key.to_string(), node.id().to_string()))
                    .or_insert(0) += 1;
            }
        }
    }

    for key in &hot_keys {
        let nodes_serving_key: HashSet<_> = hot_key_distribution
            .iter()
            .filter(|((k, _), _)| k == key)
            .map(|((_, node), _)| node.clone())
            .collect();

        assert_eq!(
            nodes_serving_key.len(),
            1,
            "Hot key '{}' should always map to the same node",
            key
        );
    }

    let unique_nodes: HashSet<_> = hot_key_distribution.iter().map(|((_, node), _)| node.clone()).collect();

    assert!(
        unique_nodes.len() >= 3,
        "Hot keys should be distributed across multiple nodes, got: {:?}",
        unique_nodes
    );
}
