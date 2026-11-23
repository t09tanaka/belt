//! The in-process pins on received sources (spec 25.8 5b (h)7).
//!
//! A reader that can outlive its attempt (the `prepare` thread, whose
//! waiter may give up; a setup builder's thread, which the release does not
//! join) pins the source it reads: it confirms under the shared mutation lock
//! that the source is in the store, then counts one pin, and the pin goes when
//! the guard is dropped (at the end of the thread's work, on a panic too).
//! The deleting pass, in this process, treats a pinned source as referenced.
//! The pins live in memory only: when the agent ends, its threads end too.

use crate::source::SourceStore;
use anyhow::{ensure, Result};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// The pins of one agent, shared by its readers and its deleting pass.
#[derive(Clone, Default)]
pub struct SourcePins(Arc<Mutex<HashMap<String, u32>>>);

/// One pin on a source; dropping it removes the pin.
pub struct Pin {
    pins: SourcePins,
    source: String,
}

impl SourcePins {
    /// Pin `source` (a CI source id) once it is confirmed to be in `store`.
    /// Waits for a deleting pass's step as the attempt's readers do; fails
    /// when the source is not in the store.
    pub fn pin(&self, store: &SourceStore, source: &str) -> Result<Pin> {
        let held = store.mutation_guard_waiting()?;
        ensure!(
            store.received(&held, source)?,
            "source {source} is not in this worker's store"
        );
        *self.lock().entry(source.to_string()).or_insert(0) += 1;
        drop(held);
        Ok(Pin {
            pins: self.clone(),
            source: source.to_string(),
        })
    }

    /// Whether some reader holds a pin on `source`.
    pub fn pinned(&self, source: &str) -> bool {
        self.lock().contains_key(source)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, u32>> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl Drop for Pin {
    fn drop(&mut self) {
        let mut pins = self.pins.lock();
        if let Some(n) = pins.get_mut(&self.source) {
            *n -= 1;
            if *n == 0 {
                pins.remove(&self.source);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pin_needs_the_source_and_goes_with_its_guard_even_on_a_panic() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let store = SourceStore::open(dir.path()).unwrap();
        let pins = SourcePins::default();
        let id = "a".repeat(64);
        assert!(pins.pin(&store, &id).is_err());
        assert!(!pins.pinned(&id));
        // (a legacy manifest file counts as present while the legacy
        // registration is pending)
        let path = store.object_store().unwrap().manifest_path(&id);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"{}").unwrap();
        let first = pins.pin(&store, &id).unwrap();
        let second = pins.pin(&store, &id).unwrap();
        drop(first);
        assert!(pins.pinned(&id));
        drop(second);
        assert!(!pins.pinned(&id));
        let (thread_pins, thread_store, thread_id) = (pins.clone(), store.clone(), id.clone());
        let ended = std::thread::spawn(move || {
            let _pin = thread_pins.pin(&thread_store, &thread_id).unwrap();
            panic!("the reader's thread ends early");
        })
        .join();
        assert!(ended.is_err());
        assert!(!pins.pinned(&id));
    }
}
