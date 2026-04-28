//! Ring Index section for `.grafeo` container persistence.
//!
//! Serializes and deserializes the [`super::TripleRing`] via the
//! [`Section`] trait, enabling the Ring to survive database restarts
//! without rebuilding from triples.
//!
//! ## Format versioning (Phase 6g)
//!
//! The section transparently handles two on-disk formats:
//!
//! - **v2 packed (current):** four packed sub-formats composed under a
//!   `GRFR` envelope with a CRC32 trailer. Reads are mmap-friendly via
//!   `Bytes::from_owner` + per-level `BitVector::from_mmap`. Writes
//!   always use this format.
//! - **v1 bincode (legacy):** preserved as a one-release fallback so
//!   existing `.grafeo` files keep loading after upgrade. Detected by
//!   the absence of the `GRFR` magic at offset 0; data flows through
//!   `TripleRing::load_from_bytes`.
//!
//! On the next checkpoint after a v1→v2 read, the section serializes
//! the in-memory ring as v2, completing the migration.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use grafeo_common::storage::section::{Section, SectionType};
use grafeo_common::utils::error::{Error, Result};

use crate::graph::rdf::RdfStore;

/// On-disk version: bumped from 1 (bincode) to 2 (packed) in Phase 6g.
const RING_SECTION_VERSION: u8 = 2;

/// First 4 bytes of the v2 envelope; absent in v1 bincode output.
const V2_MAGIC: &[u8; 4] = b"GRFR";

/// Section implementation for the RDF Ring Index.
///
/// Wraps an `Arc<RdfStore>` and serializes/deserializes the Ring via
/// `TripleRing::save_to_bytes()`/`load_from_bytes()`.
pub struct RdfRingSection {
    store: Arc<RdfStore>,
    dirty: AtomicBool,
}

impl RdfRingSection {
    /// Creates a new Ring section backed by the given RDF store.
    #[must_use]
    pub fn new(store: Arc<RdfStore>) -> Self {
        Self {
            store,
            dirty: AtomicBool::new(false),
        }
    }

    /// Marks the section as dirty (Ring was rebuilt or invalidated).
    pub fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Release);
    }
}

impl Section for RdfRingSection {
    fn section_type(&self) -> SectionType {
        SectionType::RdfRing
    }

    fn version(&self) -> u8 {
        RING_SECTION_VERSION
    }

    fn serialize(&self) -> Result<Vec<u8>> {
        match self.store.ring() {
            Some(ring) => Ok(super::serialize_triple_ring(&ring)),
            None => Ok(Vec::new()),
        }
    }

    fn deserialize(&mut self, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        // Phase 6g: detect v2 packed vs v1 bincode by magic bytes.
        let ring = if data.len() >= 4 && &data[0..4] == V2_MAGIC {
            super::deserialize_triple_ring(bytes::Bytes::copy_from_slice(data))
                .map_err(|e| Error::Serialization(e.to_string()))?
        } else {
            // v1 fallback: bincode-encoded TripleRing. Existing files keep
            // loading; the next checkpoint flushes them out as v2.
            super::TripleRing::load_from_bytes(data)
                .map_err(|e| Error::Serialization(e.to_string()))?
        };
        self.store.set_ring(ring);
        Ok(())
    }

    fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    fn mark_clean(&self) {
        self.dirty.store(false, Ordering::Release);
    }

    fn memory_usage(&self) -> usize {
        self.store.ring().map_or(0, |r| r.size_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::rdf::{Term, Triple};

    fn test_store() -> Arc<RdfStore> {
        let store = Arc::new(RdfStore::new());
        store.bulk_load(vec![
            Triple::new(
                Term::iri("http://ex.org/alix"),
                Term::iri("http://xmlns.com/foaf/0.1/name"),
                Term::literal("Alix"),
            ),
            Triple::new(
                Term::iri("http://ex.org/gus"),
                Term::iri("http://xmlns.com/foaf/0.1/name"),
                Term::literal("Gus"),
            ),
            Triple::new(
                Term::iri("http://ex.org/alix"),
                Term::iri("http://xmlns.com/foaf/0.1/knows"),
                Term::iri("http://ex.org/gus"),
            ),
        ]);
        store
    }

    #[test]
    fn section_type_is_rdf_ring() {
        let store = test_store();
        let section = RdfRingSection::new(store);
        assert_eq!(section.section_type(), SectionType::RdfRing);
        // Phase 6g: bumped from 1 (bincode) to 2 (packed).
        assert_eq!(section.version(), 2);
    }

    #[test]
    fn section_dirty_tracking() {
        let store = test_store();
        let section = RdfRingSection::new(store);
        assert!(!section.is_dirty());
        section.mark_dirty();
        assert!(section.is_dirty());
        section.mark_clean();
        assert!(!section.is_dirty());
    }

    #[test]
    fn section_serialize_empty() {
        let store = Arc::new(RdfStore::new());
        let section = RdfRingSection::new(store);
        let bytes = section.serialize().unwrap();
        assert!(bytes.is_empty());
    }

    #[test]
    fn section_roundtrip() {
        let store = test_store();
        let section = RdfRingSection::new(Arc::clone(&store));

        // Serialize
        let bytes = section.serialize().unwrap();
        assert!(!bytes.is_empty());

        // Create a fresh store and deserialize into it
        let store2 = Arc::new(RdfStore::new());
        let mut section2 = RdfRingSection::new(Arc::clone(&store2));
        section2.deserialize(&bytes).unwrap();

        // The loaded ring should have the same triple count
        let ring = store2.ring().expect("ring should be loaded");
        assert_eq!(ring.len(), 3);

        // Verify count operations work
        use crate::graph::rdf::TriplePattern;
        let name_pattern = TriplePattern {
            subject: None,
            predicate: Some(Term::iri("http://xmlns.com/foaf/0.1/name")),
            object: None,
        };
        assert_eq!(ring.count(&name_pattern), 2);
    }

    #[test]
    fn section_memory_usage() {
        let store = test_store();
        let section = RdfRingSection::new(store);
        assert!(section.memory_usage() > 0);
    }

    // ── Phase 6g: format detection + v1 → v2 migration ───────────────

    /// New writes produce a v2 buffer (starts with `GRFR` magic).
    #[test]
    fn alix_section_serialize_writes_v2_magic() {
        let store = test_store();
        let section = RdfRingSection::new(store);
        let bytes = section.serialize().unwrap();
        assert!(bytes.len() > 4);
        assert_eq!(&bytes[0..4], V2_MAGIC, "new writes must use v2 magic");
    }

    /// v1 bincode-encoded buffers still deserialize correctly (one-release
    /// fallback). The check uses save_to_bytes which produces v1 format
    /// directly — guaranteeing the migration path works for files written
    /// by older Grafeo versions.
    #[test]
    fn gus_section_v1_bincode_buffer_still_loads() {
        let original = test_store();
        let ring = original.ring().expect("ring built").as_ref().clone();
        // Encode as v1 bincode directly (bypass the section entry point).
        let v1_bytes = ring.save_to_bytes().unwrap();
        // Sanity: v1 bytes do NOT start with GRFR.
        assert_ne!(
            &v1_bytes[0..4],
            V2_MAGIC,
            "v1 bincode must not have GRFR magic"
        );

        // Deserialize via the section: should detect v1 and use the
        // bincode path.
        let store2 = Arc::new(RdfStore::new());
        let mut section2 = RdfRingSection::new(Arc::clone(&store2));
        section2.deserialize(&v1_bytes).unwrap();
        let restored = store2.ring().expect("ring loaded");
        assert_eq!(restored.len(), 3);
    }

    /// After a v1 read + a re-serialize, the new buffer is v2.
    /// Demonstrates the on-checkpoint migration.
    #[test]
    fn vincent_section_v1_then_resersialize_yields_v2() {
        let original = test_store();
        let ring = original.ring().expect("ring built").as_ref().clone();
        let v1_bytes = ring.save_to_bytes().unwrap();

        let store2 = Arc::new(RdfStore::new());
        let mut section2 = RdfRingSection::new(Arc::clone(&store2));
        section2.deserialize(&v1_bytes).unwrap();

        // Re-serialize: now in v2.
        let v2_bytes = section2.serialize().unwrap();
        assert_eq!(&v2_bytes[0..4], V2_MAGIC, "post-migration write is v2");

        // And v2 round-trips cleanly.
        let store3 = Arc::new(RdfStore::new());
        let mut section3 = RdfRingSection::new(Arc::clone(&store3));
        section3.deserialize(&v2_bytes).unwrap();
        assert_eq!(store3.ring().unwrap().len(), 3);
    }
}
