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

/// Blake2 hasher implementation (pure Rust, cryptographic)
#[cfg(feature = "hash-blake2")]
#[derive(Clone)]
pub struct Blake2Hasher;

#[cfg(feature = "hash-blake2")]
impl ConsistentHasher for Blake2Hasher {
    fn new() -> Self {
        Blake2Hasher
    }

    fn hash(&self, data: &[u8]) -> u64 {
        use blake2::{Blake2b, Digest};
        let mut hasher = Blake2b::<typenum::U8>::new();
        hasher.update(data);
        let result = hasher.finalize();
        u64::from_le_bytes(result.into())
    }

    fn name(&self) -> &'static str {
        "blake2"
    }
}

/// SHA-256 hasher implementation (FIPS compliant)
#[cfg(feature = "hash-sha2")]
#[derive(Clone)]
pub struct Sha256Hasher;

#[cfg(feature = "hash-sha2")]
impl ConsistentHasher for Sha256Hasher {
    fn new() -> Self {
        Sha256Hasher
    }

    fn hash(&self, data: &[u8]) -> u64 {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(data);
        let result = hasher.finalize();
        u64::from_le_bytes([
            result[0], result[1], result[2], result[3], result[4], result[5], result[6], result[7],
        ])
    }

    fn name(&self) -> &'static str {
        "sha256"
    }
}

/// FNV-1a hasher implementation (simple, fast, built-in)
#[cfg(feature = "hash-fnv")]
#[derive(Clone)]
pub struct FnvHasher;

#[cfg(feature = "hash-fnv")]
impl ConsistentHasher for FnvHasher {
    fn new() -> Self {
        FnvHasher
    }

    fn hash(&self, data: &[u8]) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;
        let mut hasher = DefaultHasher::new();
        hasher.write(data);
        hasher.finish()
    }

    fn name(&self) -> &'static str {
        "fnv"
    }
}

/// Default hasher type based on feature flags
#[cfg(feature = "hash-blake3")]
pub type DefaultHasher = Blake3Hasher;

#[cfg(all(feature = "hash-xxhash", not(feature = "hash-blake3")))]
pub type DefaultHasher = XXHasher;

#[cfg(all(feature = "hash-blake2", not(feature = "hash-blake3"), not(feature = "hash-xxhash")))]
pub type DefaultHasher = Blake2Hasher;

#[cfg(all(
    feature = "hash-sha2",
    not(feature = "hash-blake3"),
    not(feature = "hash-xxhash"),
    not(feature = "hash-blake2")
))]
pub type DefaultHasher = Sha256Hasher;

#[cfg(all(
    feature = "hash-fnv",
    not(feature = "hash-blake3"),
    not(feature = "hash-xxhash"),
    not(feature = "hash-blake2"),
    not(feature = "hash-sha2")
))]
pub type DefaultHasher = FnvHasher;

#[cfg(test)]
mod tests {
    use super::*;

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
    #[cfg(any(
        feature = "hash-blake3",
        feature = "hash-xxhash",
        feature = "hash-blake2",
        feature = "hash-sha2",
        feature = "hash-fnv"
    ))]
    fn default_hasher() {
        let hasher = DefaultHasher::new();
        let data = b"consistent hashing test";

        let hash = hasher.hash(data);
        assert_ne!(hash, 0, "Default hasher should produce non-zero hash");

        println!("Using hasher: {}", hasher.name());
    }
}
