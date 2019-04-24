use std::sync::atomic::{AtomicUsize, Ordering};

use data::ID;

#[derive(Debug)]
pub struct IDCounter {
    store: AtomicUsize,
}

impl IDCounter {
    pub fn new(init: usize) -> Self {
        IDCounter {
            store: AtomicUsize::new(init),
        }
    }

    pub fn get(&self) -> ID {
        ID::new(self.store.fetch_add(1, Ordering::Relaxed) as u64)
    }

    pub fn snapshot(&self) -> Self {
        IDCounter {
            store: AtomicUsize::new(self.store.load(Ordering::Relaxed)),
        }
    }
}

#[derive(Debug)]
pub struct IDWrap<'a> {
    inner: &'a mut IDCounter,
    cur: IDCounter,
}

impl<'a> IDWrap<'a> {
    pub fn new(inner: &'a mut IDCounter) -> Self {
        let cur = inner.snapshot();
        IDWrap { inner, cur }
    }

    pub fn get(&self) -> ID {
        self.cur.get()
    }

    pub fn commit(self) {
        self.inner
            .store
            .store(self.cur.store.load(Ordering::SeqCst), Ordering::SeqCst);
    }
}
