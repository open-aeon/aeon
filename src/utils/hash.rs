use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

/// Calculates the hash value (u64) of any object that implements the `Hash` trait.
///
/// # Example
/// ```
/// let value = "hello";
/// let hash = calculate_hash(&value);
/// println!("Hash: {}", hash);
/// ```
pub fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    t.hash(&mut hasher);
    hasher.finish()
}