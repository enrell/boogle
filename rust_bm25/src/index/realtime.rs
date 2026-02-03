use crate::index::ram::{Document, RamIndex};
use crate::index::wal::Wal;
use crate::search::searcher::FileSearcher;
use pyo3::prelude::*;
use std::cmp::Ordering;
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
        mem.clear();

        self.wal
            .lock()
            .unwrap()
            .truncate()
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

        Ok(count)
    }
}
