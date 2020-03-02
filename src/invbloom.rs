//! Reverse bloom filter based on
//!
//! https://www.somethingsimilar.com/2012/05/21/the-opposite-of-a-bloom-filter/

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::atomic::{AtomicUsize, Ordering},
};

const N: usize = 256; // have to pick power of 2
const NMASK: usize = N - 1;

#[derive(Default)]
pub struct InvBloom {
    data: Vec<AtomicUsize>,
}

impl InvBloom {
    pub fn new() -> InvBloom {
        let mut data = Vec::with_capacity(N);
        for _ in 0..N {
            data.push(AtomicUsize::new(0));
        }
        InvBloom { data: data }
    }

    pub fn check<T: Hash>(&self, test: &T) -> bool {
        let hash = {
            let mut hasher = DefaultHasher::new();
            test.hash(&mut hasher);
            hasher.finish() as usize
        };
        let prev = self.data[hash & NMASK].swap(hash, Ordering::Relaxed);
        prev == hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creation() {
        InvBloom::new();
    }

    #[test]
    fn check() {
        let ib = InvBloom::new();
        assert_eq!(ib.check(&1), false);
    }

    #[test]
    fn not_seen_before() {
        let ib = InvBloom::new();
        for i in 0..100000 {
            assert_eq!(ib.check(&i), false);
        }
    }

    #[test]
    fn seen_before() {
        let ib = InvBloom::new();
        assert_eq!(ib.check(&1), false);
        assert_eq!(ib.check(&1), true);
        assert_eq!(ib.check(&1), true);
        assert_eq!(ib.check(&2), false);
        assert_eq!(ib.check(&2), true);
        assert_eq!(ib.check(&2), true);
    }
}
