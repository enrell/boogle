use crate::analysis::analyze;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

const K1: f32 = 1.2;
const B: f32 = 0.75;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Document {
    pub id: u32,
    pub content: String,
    pub metadata: String,
    pub length: u32,
}

pub struct RamIndex {
    pub inverted_index: FxHashMap<String, Vec<(u32, u32)>>,
    pub docs: FxHashMap<u32, Document>,
    pub next_doc_id: u32,
    pub total_length: u64,
}

impl RamIndex {
    pub fn new(start_doc_id: u32) -> Self {
        Self {
            inverted_index: FxHashMap::default(),
            docs: FxHashMap::default(),
            next_doc_id: start_doc_id,
            total_length: 0,
        }
    }

    pub fn insert(&mut self, content: String, metadata: String) -> u32 {
        let doc_id = self.next_doc_id;
        self.next_doc_id += 1;

        let tokens = analyze(&content);
        let doc_length = tokens.len() as u32;
        self.total_length += doc_length as u64;

        self.docs.insert(
            doc_id,
            Document {
                id: doc_id,
                content: content.clone(),
                metadata,
                length: doc_length,
            },
        );

        let mut term_freqs: FxHashMap<String, u32> = FxHashMap::default();
        for token in tokens {
            *term_freqs.entry(token).or_insert(0) += 1;
        }

        for (term, freq) in term_freqs {
            self.inverted_index
                .entry(term)
                .or_default()
                .push((doc_id, freq));
        }

        doc_id
    }

    pub fn search(&self, query: &str) -> Vec<(u32, f32)> {
        let tokens = analyze(query);
        if tokens.is_empty() {
            return vec![];
        }

        let avgdl = if self.docs.is_empty() {
            0.0
        } else {
            self.total_length as f32 / self.docs.len() as f32
        };

        let mut scores: FxHashMap<u32, f32> = FxHashMap::default();

        for token in tokens {
            if let Some(postings) = self.inverted_index.get(&token) {
                let idf = self.compute_idf(postings.len() as f32);

                for &(doc_id, freq) in postings {
                    let doc = self.docs.get(&doc_id).unwrap();
                    let score = self.bm25_score(freq as f32, doc.length as f32, avgdl, idf);
                    *scores.entry(doc_id).or_insert(0.0) += score;
                }
            }
        }

        let mut results: Vec<_> = scores.into_iter().collect();
        results.sort_unstable_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
        results
    }

    fn compute_idf(&self, df: f32) -> f32 {
        let n = self.docs.len() as f32;
        ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
    }

    fn bm25_score(&self, tf: f32, doc_len: f32, avgdl: f32, idf: f32) -> f32 {
        let numerator = tf * (K1 + 1.0);
        let denominator = tf + K1 * (1.0 - B + B * (doc_len / avgdl));
        idf * numerator / denominator
    }

    pub fn clear(&mut self) {
        self.inverted_index.clear();
        self.docs.clear();
        self.total_length = 0;
    }
}
