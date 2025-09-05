//! Hash function abstraction for consistent hashing
//!
//! This module provides a trait for hash functions and implementations
//! for various hashing algorithms that can be selected via feature flags.

/// Trait for hash functions used in consistent hashing
pub trait ConsistentHasher: Clone + Send + Sync {
    /// Create a new hasher instance
    fn new() -> Self;

    /// Hash a byte slice and return a 64-bit integer for fast BTree operations
    fn hash(&self, data: &[u8]) -> u64;

    /// Get the name of this hasher for diagnostics
    fn name(&self) -> &'static str;
}

/// Blake3 hasher implementation (default)
#[cfg(feature = "hash-blake3")]
#[derive(Clone)]
pub struct Blake3Hasher;

#[cfg(feature = "hash-blake3")]
impl ConsistentHasher for Blake3Hasher {
    fn new() -> Self {
        Blake3Hasher
    }

    fn hash(&self, data: &[u8]) -> u64 {
        // Convert first 8 bytes of blake3 hash to u64 for fast BTree operations
        let hash = blake3::hash(data);
        let bytes = hash.as_bytes();
        u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ])
    }

    fn name(&self) -> &'static str {
        "blake3"
    }
}

/// XXHash hasher implementation (fastest, non-cryptographic)
#[cfg(feature = "hash-xxhash")]
#[derive(Clone)]
pub struct XXHasher;

#[cfg(feature = "hash-xxhash")]
impl ConsistentHasher for XXHasher {
    fn new() -> Self {
        XXHasher
    }

    fn hash(&self, data: &[u8]) -> u64 {
        use xxhash_rust::xxh3::xxh3_64;
        xxh3_64(data)
    }

    fn name(&self) -> &'static str {
        "xxhash"
    }
}

/// MetroHash hasher implementation (fast, high-quality)
#[cfg(feature = "hash-metrohash")]
#[derive(Clone)]
pub struct MetroHasher;

#[cfg(feature = "hash-metrohash")]
impl ConsistentHasher for MetroHasher {
    fn new() -> Self {
        MetroHasher
    }

    fn hash(&self, data: &[u8]) -> u64 {
        use std::hash::Hasher;

        use metrohash::MetroHash64;
        let mut hasher = MetroHash64::new();
        hasher.write(data);
        hasher.finish()
    }

    fn name(&self) -> &'static str {
        "metrohash"
    }
}

/// RapidHash quality hasher implementation (high-quality distribution)
#[cfg(feature = "hash-rapidhash")]
#[derive(Clone)]
pub struct RapidHashQualityHasher;

#[cfg(feature = "hash-rapidhash")]
impl ConsistentHasher for RapidHashQualityHasher {
    fn new() -> Self {
        RapidHashQualityHasher
    }

    fn hash(&self, data: &[u8]) -> u64 {
        use std::hash::BuildHasher;

        use rapidhash::quality::SeedableState;
        let hasher = SeedableState::fixed();
        hasher.hash_one(data)
    }

    fn name(&self) -> &'static str {
        "rapidhash-quality"
    }
}

/// RapidHash fast hasher implementation (optimized for speed)
#[cfg(feature = "hash-rapidhash")]
#[derive(Clone)]
pub struct RapidHashFastHasher;

#[cfg(feature = "hash-rapidhash")]
impl ConsistentHasher for RapidHashFastHasher {
    fn new() -> Self {
        RapidHashFastHasher
    }

    fn hash(&self, data: &[u8]) -> u64 {
        use std::hash::BuildHasher;

        use rapidhash::fast::SeedableState;
        let hasher = SeedableState::fixed();
        hasher.hash_one(data)
    }

    fn name(&self) -> &'static str {
        "rapidhash-fast"
    }
}

/// Murmur3 hasher implementation (popular, good distribution)
#[cfg(feature = "hash-murmur3")]
#[derive(Clone)]
pub struct Murmur3Hasher;

#[cfg(feature = "hash-murmur3")]
impl ConsistentHasher for Murmur3Hasher {
    fn new() -> Self {
        Murmur3Hasher
    }

    fn hash(&self, data: &[u8]) -> u64 {
        murmurs::murmur3_x64_128(data, 0)[0]
    }

    fn name(&self) -> &'static str {
        "murmur3"
    }
}

/// Default hasher type based on feature flags. Since somebody might turn on all the hashes
/// at once, we fall through them in the order optimized for rendezvous hashing performance.
/// Based on comprehensive testing, Murmur3 provides the best balance of speed and distribution quality.
#[cfg(feature = "hash-murmur3")]
pub type DefaultHasher = Murmur3Hasher;

#[cfg(all(feature = "hash-xxhash", not(feature = "hash-murmur3")))]
pub type DefaultHasher = XXHasher;

#[cfg(all(
    feature = "hash-rapidhash",
    not(feature = "hash-murmur3"),
    not(feature = "hash-xxhash")
))]
pub type DefaultHasher = RapidHashFastHasher;

#[cfg(all(
    feature = "hash-metrohash",
    not(feature = "hash-murmur3"),
    not(feature = "hash-xxhash"),
    not(feature = "hash-rapidhash")
))]
pub type DefaultHasher = MetroHasher;

#[cfg(all(
    feature = "hash-blake3",
    not(feature = "hash-murmur3"),
    not(feature = "hash-xxhash"),
    not(feature = "hash-rapidhash"),
    not(feature = "hash-metrohash")
))]
pub type DefaultHasher = Blake3Hasher;

#[cfg(test)]
mod tests {

    #[test]
    #[cfg(feature = "hash-blake3")]
    fn blake3_consistency() {
        let hasher = Blake3Hasher::new();
        let data = b"test data";

        let hash1 = hasher.hash(data);
        let hash2 = hasher.hash(data);

        assert_eq!(hash1, hash2, "Hash should be consistent");
        assert_ne!(hash1, 0, "Hash should not be zero");
    }

    #[test]
    #[cfg(feature = "hash-blake3")]
    fn blake3_different_inputs() {
        let hasher = Blake3Hasher::new();

        let hash1 = hasher.hash(b"test1");
        let hash2 = hasher.hash(b"test2");

        assert_ne!(hash1, hash2, "Different inputs should produce different hashes");
    }

    #[test]
    #[cfg(any(feature = "hash-blake3", feature = "hash-xxhash"))]
    fn default_hasher() {
        let hasher = DefaultHasher::new();
        let data = b"consistent hashing test";

        let hash = hasher.hash(data);
        assert_ne!(hash, 0, "Default hasher should produce non-zero hash");

        println!("Using hasher: {}", hasher.name());
    }
}
