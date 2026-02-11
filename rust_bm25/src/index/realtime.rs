use crate::analysis::analyze;
use crate::index::ram::{Document, RamIndex};
use crate::index::segment::{BatchData, IndexMeta, ProcessedDoc, SegmentMeta};
use crate::index::wal::Wal;
use crate::index::writer::write_segment;
use crate::search::searcher::FileSearcher;
use pyo3::prelude::*;
use std::cmp::Ordering;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

#[pyclass]
pub struct RealTimeIndexer {
    #[allow(dead_code)]
    index_dir: String,
    disk_index: Arc<RwLock<FileSearcher>>,
    memory_index: Arc<RwLock<RamIndex>>,
    wal: Arc<Mutex<Wal>>,
}

#[pymethods]
impl RealTimeIndexer {
    #[new]
    fn new(index_dir: String) -> PyResult<Self> {
        let path = PathBuf::from(&index_dir);
        let wal_path = path.join("index.wal");

        let wal = Wal::open(&wal_path)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let recovered_docs = wal.read_all().unwrap_or_default();

        let disk = FileSearcher::new(&index_dir)?;
        let mut ram = RamIndex::new(disk.num_docs());

        for doc in recovered_docs {
            ram.insert(doc.content, doc.metadata);
        }

        Ok(Self {
            index_dir,
            disk_index: Arc::new(RwLock::new(disk)),
            memory_index: Arc::new(RwLock::new(ram)),
            wal: Arc::new(Mutex::new(wal)),
        })
    }

    fn add_document(&self, content: String, metadata: String) -> PyResult<u32> {
        let mut mem = self.memory_index.write().unwrap();
        let doc_id = mem.insert(content.clone(), metadata.clone());

        let doc_for_wal = Document {
            id: doc_id,
            content,
            metadata,
            length: 0,
        };

        self.wal
            .lock()
            .unwrap()
            .append(&doc_for_wal)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

        Ok(doc_id)
    }

    fn search(&self, query: String, top_k: usize) -> Vec<(String, f32, u32)> {
        let disk = self.disk_index.read().unwrap();
        let mem = self.memory_index.read().unwrap();

        let (disk_results, mem_results) =
            rayon::join(|| disk.search(&query, top_k), || mem.search(&query));

        let mut results = disk_results;

        for (doc_id, score) in mem_results {
            if mem.docs.contains_key(&doc_id) {
                results.push(("RAM_BOOK".to_string(), score, doc_id));
            }
        }

        results.sort_unstable_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
        results.truncate(top_k);
        results
    }

    fn flush(&self) -> PyResult<u32> {
        let mut mem = self.memory_index.write().unwrap();
        if mem.docs.is_empty() {
            return Ok(0);
        }

        let count = mem.docs.len() as u32;

        // Get current disk index info for base_doc_id
        let base_doc_id = {
            let disk = self.disk_index.read().unwrap();
            disk.num_docs()
        };

        // Convert memory documents to ProcessedDoc format
        let docs: Vec<ProcessedDoc> = mem
            .docs
            .values()
            .map(|doc| {
                let tokens = analyze(&doc.content);
                let doc_length = tokens.len() as u32;

                // Build frequency map
                let mut term_freqs: rustc_hash::FxHashMap<String, u32> =
                    rustc_hash::FxHashMap::default();
                for token in tokens {
                    *term_freqs.entry(token).or_insert(0) += 1;
                }

                // Create a single chunk for the document
                let chunks = vec![(doc_length, term_freqs)];

                ProcessedDoc {
                    book_id: doc.metadata.clone(), // Use metadata as book_id
                    chunks,
                }
            })
            .collect();

        // Read current index meta to get segment count
        let index_path = PathBuf::from(&self.index_dir);
        let meta_path = index_path.join("index.json");

        let mut meta: IndexMeta = fs::read_to_string(&meta_path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or(IndexMeta {
                segments: vec![],
                total_docs: base_doc_id,
                avgdl: 0.0,
            });

        let segment_id = meta.segments.len();
        let segment_name = format!("segment_{}", segment_id);
        let segment_dir = index_path.join(&segment_name);

        // Create BatchData for the segment
        let batch_data = BatchData {
            segment_id,
            segment_dir: segment_dir.clone(),
            docs,
            base_doc_id,
        };

        // Write the segment
        let segment_meta = write_segment(batch_data).map_err(|e| {
            pyo3::exceptions::PyIOError::new_err(format!("Failed to write segment: {}", e))
        })?;

        // Update index meta
        meta.segments.push(segment_name);
        meta.total_docs += segment_meta.num_docs;
        meta.avgdl = if meta.total_docs > 0 {
            (base_doc_id as f32 + count as f32) / meta.total_docs as f32
        } else {
            0.0
        };

        fs::write(
            &meta_path,
            serde_json::to_string(&meta).map_err(|e| {
                pyo3::exceptions::PyIOError::new_err(format!("Failed to serialize meta: {}", e))
            })?,
        )
        .map_err(|e| {
            pyo3::exceptions::PyIOError::new_err(format!("Failed to write meta: {}", e))
        })?;

        // Clear memory index
        mem.clear();

        // Reload disk index
        let new_disk = FileSearcher::new(&self.index_dir).map_err(|e| {
            pyo3::exceptions::PyIOError::new_err(format!("Failed to reload index: {}", e))
        })?;

        {
            let mut disk = self.disk_index.write().unwrap();
            *disk = new_disk;
        }

        // Truncate WAL
        self.wal
            .lock()
            .unwrap()
            .truncate()
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

        Ok(count)
    }
}
