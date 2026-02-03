use crate::analysis::analyze;
use crate::index::reader::SegmentReader;
use crate::index::segment::IndexMeta;
use pyo3::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use std::cmp::Ordering;
use std::fs;
use std::path::Path;

const K1: f32 = 1.5;
const B: f32 = 0.75;

#[pyclass]
pub struct FileSearcher {
    segments: Vec<SegmentReader>,
    total_docs: u32,
    avgdl: f32,
    stopwords: FxHashSet<String>,
}

#[pymethods]
impl FileSearcher {
    #[new]
    pub fn new(index_dir: &str) -> PyResult<Self> {
        let path = Path::new(index_dir);
        let meta: IndexMeta =
            serde_json::from_str(&fs::read_to_string(path.join("index.json")).map_err(|e| {
                pyo3::exceptions::PyIOError::new_err(format!("Cannot read index.json: {}", e))
            })?)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

        let segments = meta
            .segments
            .iter()
            .map(|name| {
                SegmentReader::open(&path.join(name)).map_err(|e| {
                    pyo3::exceptions::PyIOError::new_err(format!("Cannot open {}: {}", name, e))
                })
            })
            .collect::<PyResult<Vec<_>>>()?;

        Ok(Self {
            segments,
            total_docs: meta.total_docs,
            avgdl: meta.avgdl,
            stopwords: FxHashSet::default(),
        })
    }

    fn set_stopwords(&mut self, words: Vec<String>) {
        self.stopwords = words.into_iter().collect();
    }

    #[getter]
    pub fn num_docs(&self) -> u32 {
        self.total_docs
    }

    #[getter]
    fn avgdl(&self) -> f32 {
        self.avgdl
    }

    pub fn search(&self, query: &str, top_k: usize) -> Vec<(String, f32, u32)> {
        let tokens: Vec<_> = analyze(query)
            .into_iter()
            .filter(|t| !self.stopwords.contains(t))
            .collect();

        if tokens.is_empty() {
            return vec![];
        }

        let mut doc_scores: FxHashMap<u32, f32> = FxHashMap::default();

        for token in tokens {
            self.score_token(&token, &mut doc_scores);
        }

        self.select_top_k(doc_scores, top_k)
    }

    fn get_book_id(&self, chunk_id: u32) -> Option<String> {
        self.segments.iter().find_map(|s| s.get_book_id(chunk_id))
    }
}

impl FileSearcher {
    fn score_token(&self, token: &str, doc_scores: &mut FxHashMap<u32, f32>) {
        let (search_tokens, total_df) = self.resolve_term(token);

        if total_df == 0 {
            return;
        }

        let idf = self.compute_idf(total_df);

        for term in search_tokens {
            for segment in &self.segments {
                if let Some(iter) = segment.get_postings_iter(&term) {
                    for (doc_id, tf) in iter {
                        let doc_len = segment.get_doc_length(doc_id).unwrap_or(1) as f32;
                        let score = self.bm25_score(tf as f32, doc_len, idf);
                        *doc_scores.entry(doc_id).or_insert(0.0) += score;
                    }
                }
            }
        }
    }

    fn resolve_term(&self, token: &str) -> (Vec<String>, u32) {
        let mut total_df = 0u32;

        for segment in &self.segments {
            if let Some(df) = segment.get_doc_freq(token) {
                total_df += df;
            }
        }

        if total_df > 0 {
            return (vec![token.to_string()], total_df);
        }

        let dist = if token.len() > 4 { 2 } else { 1 };
        let mut candidates = FxHashSet::default();

        for segment in &self.segments {
            candidates.extend(segment.get_fuzzy_terms(token, dist));
        }

        if candidates.is_empty() {
            return (vec![], 0);
        }

        let tokens: Vec<_> = candidates.into_iter().collect();
        let total_df = tokens
            .iter()
            .map(|t| {
                self.segments
                    .iter()
                    .filter_map(|s| s.get_doc_freq(t))
                    .sum::<u32>()
            })
            .sum();

        (tokens, total_df)
    }

    fn compute_idf(&self, df: u32) -> f32 {
        let n = self.total_docs as f32;
        let df = df as f32;
        ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
    }

    fn bm25_score(&self, tf: f32, doc_len: f32, idf: f32) -> f32 {
        let numerator = tf * (K1 + 1.0);
        let denominator = tf + K1 * (1.0 - B + B * doc_len / self.avgdl);
        idf * numerator / denominator
    }

    fn select_top_k(
        &self,
        doc_scores: FxHashMap<u32, f32>,
        top_k: usize,
    ) -> Vec<(String, f32, u32)> {
        if doc_scores.is_empty() {
            return vec![];
        }

        let mut results: Vec<_> = doc_scores.into_iter().collect();
        let k = top_k.min(results.len());

        results.select_nth_unstable_by(k - 1, |a, b| {
            b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal)
        });
        results.truncate(k);
        results.sort_unstable_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));

        results
            .into_iter()
            .filter_map(|(doc_id, score)| {
                let book_id = self.segments.iter().find_map(|s| s.get_book_id(doc_id))?;
                Some((book_id, score, doc_id))
            })
            .collect()
    }
}
