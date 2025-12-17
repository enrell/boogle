use pyo3::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use std::fs;
use std::path::Path;

use crate::analysis::analyze;
use crate::index::reader::SegmentReader;
use crate::index::segment::IndexMeta;

#[pyclass]
pub struct FileSearcher {
    segments: Vec<SegmentReader>,
    total_docs: u32,
    avgdl: f32,
    k1: f32,
    b: f32,
    stopwords: FxHashSet<String>,
}

#[pymethods]
impl FileSearcher {
    #[new]
    fn new(index_dir: &str) -> PyResult<Self> {
        let path = Path::new(index_dir);
        let meta_path = path.join("index.json");

        let meta_str = fs::read_to_string(&meta_path).map_err(|e| {
            pyo3::exceptions::PyIOError::new_err(format!("Cannot read index.json: {}", e))
        })?;
        let meta: IndexMeta = serde_json::from_str(&meta_str)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

        let mut segments = Vec::with_capacity(meta.segments.len());
        for seg_name in &meta.segments {
            let seg_dir = path.join(seg_name);
            let reader = SegmentReader::open(&seg_dir).map_err(|e| {
                pyo3::exceptions::PyIOError::new_err(format!(
                    "Cannot open segment {}: {}",
                    seg_name, e
                ))
            })?;
            segments.push(reader);
        }

        Ok(Self {
            segments,
            total_docs: meta.total_docs,
            avgdl: meta.avgdl,
            k1: 1.5,
            b: 0.75,
            stopwords: FxHashSet::default(),
        })
    }

    fn set_stopwords(&mut self, words: Vec<String>) {
        self.stopwords = words.into_iter().collect();
    }

    #[getter]
    fn num_docs(&self) -> u32 {
        self.total_docs
    }

    #[getter]
    fn avgdl(&self) -> f32 {
        self.avgdl
    }

    fn search(&self, query: &str, top_k: usize) -> Vec<(String, f32, u32)> {
        let tokens: Vec<String> = analyze(query)
            .into_iter()
            .filter(|t| !self.stopwords.contains(t))
            .collect();

        if tokens.is_empty() {
            return vec![];
        }

        let mut doc_scores: FxHashMap<u32, f32> = FxHashMap::default();

        for token in &tokens {
            let mut total_df = 0u32;
            let mut all_postings: Vec<(u32, u32)> = Vec::new();

            for segment in &self.segments {
                if let Some(postings) = segment.get_postings(token) {
                    total_df += postings.len() as u32;
                    all_postings.extend(postings);
                }
            }

            if total_df == 0 {
                continue;
            }

            let idf = ((self.total_docs as f32 - total_df as f32 + 0.5) / (total_df as f32 + 0.5)
                + 1.0)
                .ln();

            for (doc_id, tf) in all_postings {
                let doc_len = self
                    .segments
                    .iter()
                    .find_map(|s| s.get_doc_length(doc_id))
                    .unwrap_or(1) as f32;

                let tf_f = tf as f32;
                let numerator = tf_f * (self.k1 + 1.0);
                let denominator = tf_f + self.k1 * (1.0 - self.b + self.b * doc_len / self.avgdl);
                let score = idf * numerator / denominator;
                *doc_scores.entry(doc_id).or_insert(0.0) += score;
            }
        }

        let mut results: Vec<(u32, f32)> = doc_scores.into_iter().collect();
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(top_k);

        results
            .into_iter()
            .filter_map(|(doc_id, score)| {
                let book_id = self.segments.iter().find_map(|s| s.get_book_id(doc_id))?;
                Some((book_id, score, doc_id))
            })
            .collect()
    }

    fn get_book_id(&self, chunk_id: u32) -> Option<String> {
        self.segments.iter().find_map(|s| s.get_book_id(chunk_id))
    }
}
