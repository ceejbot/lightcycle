# LightCycle

Rust implementations of the consistent hash ring and rendezvous hash data structures, with a shared API intended to support use in making nicely-distributed clusters of things. Common use cases include distributing items among caches that might appear and disappear, while handling redistribution, or directing sticky sessions at the hosts that should handle them.

Rendezvous hashing is a more general yet simpler-to-implement variation of consistent hashing, so you should probably use that most of the time. Both data structures are handy to have around.

You may feel free to call these data structures by their more fun names of `LightCycle` and `Recognizer`, or you can be boring and call them `ConsistentRing` and `RendezvousRing`.

I haven't used these in production workloads (yet?), but they are well-tested and have reasonable performance, because that's part of the fun.

## Examples

### Using ConsistentRing (LightCycle)

```rust
use lightcycle::{HasId, ConsistentRing, HashRing};
// Or use the preferred name: use lightcycle::{HasId, LightCycle, HashRing};

#[derive(Debug, Clone)]
struct RedisCacheShard {
    uri: String,
    // In a real implementation you'd have a redis::Client here
}

impl RedisCacheShard {
    fn new(host: &str, port: u16, db: u8) -> Self {
        let uri = format!("redis://{}:{}/{}", host, port, db);
        Self { uri }
    }
}

impl HasId for RedisCacheShard {
    fn id(&self) -> &str {
        // We use the redis URI as a unique id for each shard
        &self.uri
    }
}

fn distribute_to_shards(shards: Vec<RedisCacheShard>) {
    // Create a consistent hash ring with custom replica count
    let mut ring = ConsistentRing::new_with_replica_count(5);

    // Add each cache shard to the ring
    for cache in shards {
        ring.add(Box::new(cache));
    }

    // Find the right shard for some data
    let key = "user:12345:session";
    if let Some(shard) = ring.locate(key) {
        println!("Key '{}' should be stored on shard: {}", key, shard.id());
        // In real code: shard.client.set(key, data)?;
    }
}
```

### Using RendezvousRing (Recognizer) with Weighted Nodes

```rust
use lightcycle::{HasId, RendezvousRing, HashRing};
// Or use the preferred name: use lightcycle::{HasId, Recognizer, HashRing};

#[derive(Debug, Clone)]
struct CacheNode {
    name: String,
    capacity_gb: f64,
}

impl HasId for CacheNode {
    fn id(&self) -> &str {
        &self.name
    }
}

fn weighted_distribution() {
    let mut ring = RendezvousRing::new();

    // Add cache nodes with different capacities
    let small = CacheNode { name: "cache-small".into(), capacity_gb: 8.0 };
    let large = CacheNode { name: "cache-large".into(), capacity_gb: 64.0 };

    // Weight by capacity - larger cache gets proportionally more keys
    ring.add_weighted(Box::new(small.clone()), 8.0);
    ring.add_weighted(Box::new(large.clone()), 64.0);

    // Keys will be distributed proportional to weights
    for key in ["session:1", "session:2", "session:3"] {
        if let Some(node) = ring.locate(key) {
            println!("{} -> {}", key, node.id());
        }
    }
}
```

## Choosing Between ConsistentRing and RendezvousRing

Use rendezvouz hashing when:

- You're at all uncertain about which to pick
- You want a weighted distribution based on resource capacity
- You have fewer resources (< 100) where O(n) lookup is acceptable
- Memory efficiency is important (no replica storage needed)
- You want the simplest possible implementation

Use the consistent hash ring when:

- You need the traditional consistent hashing algorithm
- You have many resources (100+) and need O(log n) lookup time
- Memory usage is not a primary concern
- You need fine control over replica placement

## API

### Common Interface (HashRing trait)

Both ring types implement the `HashRing` trait:

```rust
// Create a ring
let mut ring = ConsistentRing::new();  // or RendezvousRing::new()

// Add resources
ring.add(Box::new(resource));

// Add with weight (RendezvousRing only, ConsistentRing ignores weight)
ring.add_weighted(Box::new(resource), weight);

// Find resource for a key
let resource = ring.locate("some-key");

// Remove a resource
ring.remove(&resource);

// Update weight (RendezvousRing only, ConsistentRing returns error)
ring.update_weight(&resource, new_weight);
```

### ConsistentRing Specific

```rust
// Create with custom replica count (default is 4)
let ring = ConsistentRing::new_with_replica_count(100);

// Get replica count
let replicas = ring.replica_count();
```

### RendezvousRing Specific

```rust
// Get current weight of a resource
let weight = ring.get_weight(&resource);

// List all resources with their weights
let weighted_resources = ring.resources_with_weights();
```

### Hash Functions

The library supports multiple hash functions via feature flags:

```toml
[dependencies]
# Default uses blake3
lightcycle = "0.2"

# Use xxhash for maximum speed
lightcycle = { version = "0.2", default-features = false, features = ["hash-xxhash"] }

# Other options: hash-blake2, hash-sha2, hash-fnv
```

## Performance

Based on benchmarks with 10 nodes and 10,000 lookups:

- **RendezvousRing**: ~500μs (50ns per lookup)
- **ConsistentRing**: ~1ms (100ns per lookup)
- **Memory**: RendezvousRing uses ~100x less memory (no replica storage)

## Documentation

Run `cargo doc --open` for detailed API documentation.

## LICENSE

This code is licensed via [the Parity Public License.](https://paritylicense.com) This license requires people who build on top of this source code to share their work with the community, too. This means if you hack on it for work, you have to make your work repo public somehow. I mean, have fun. See the license text for details.
