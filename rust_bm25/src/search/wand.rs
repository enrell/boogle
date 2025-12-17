use pyo3::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::codecs::decode_postings_internal;

struct TermInfo {
    idf: f32,
    upper_bound: f32,
    postings: FxHashMap<u32, u32>,
}

#[derive(Clone, Copy)]
struct ScoredDoc {
    doc_id: u32,
    score: f32,
}

impl PartialEq for ScoredDoc {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}

impl Eq for ScoredDoc {}

impl PartialOrd for ScoredDoc {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredDoc {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .score
            .partial_cmp(&self.score)
            .unwrap_or(Ordering::Equal)
    }
}

#[pyclass]
pub struct WandSearcher {
    k1: f32,
    b: f32,
    num_docs: u32,
    avgdl: f32,
    stopwords: FxHashSet<String>,
}

#[pymethods]
impl WandSearcher {
    #[new]
    #[pyo3(signature = (num_docs, avgdl, k1=1.5, b=0.75))]
    fn new(num_docs: u32, avgdl: f32, k1: f32, b: f32) -> Self {
        Self {
            k1,
            b,
            num_docs,
            avgdl,
            stopwords: FxHashSet::default(),
        }
    }

    fn set_stopwords(&mut self, words: Vec<String>) {
        self.stopwords = words.into_iter().collect();
    }

    fn search(
        &self,
        _query: &str,
        posting_data: Vec<(u32, Vec<u8>)>,
        top_k: usize,
    ) -> Vec<(u32, f32)> {
        if posting_data.is_empty() {
            return vec![];
        }

        let mut terms: Vec<TermInfo> = posting_data
            .into_iter()
            .map(|(df, data)| {
                let idf = self.compute_idf(df);
                let upper_bound = idf * (self.k1 + 1.0);
                let postings: FxHashMap<u32, u32> =
                    decode_postings_internal(&data).into_iter().collect();
                TermInfo {
                    idf,
                    upper_bound,
                    postings,
                }
            })
            .collect();

        terms.sort_by_key(|t| t.postings.len());

        let candidate_docs = self.compute_candidates(&terms, top_k);

        // Compute doc lengths from tf sums
        let mut doc_lengths: FxHashMap<u32, u32> = FxHashMap::default();
        for term in &terms {
            for (&doc_id, &tf) in &term.postings {
                if candidate_docs.contains(&doc_id) {
                    *doc_lengths.entry(doc_id).or_insert(0) += tf;
                }
            }
        }

        // Use avgdl as minimum
        let min_len = (self.avgdl * 0.5) as u32;
        for len in doc_lengths.values_mut() {
            if *len < min_len {
                *len = self.avgdl as u32;
            }
        }

        let candidates_with_upper: Vec<(f32, u32)> = candidate_docs
            .iter()
            .filter_map(|&doc_id| {
                doc_lengths.get(&doc_id).map(|_| {
                    let upper: f32 = terms
                        .iter()
                        .filter(|t| t.postings.contains_key(&doc_id))
                        .map(|t| t.upper_bound)
                        .sum();
                    (upper, doc_id)
                })
            })
            .collect();

        self.wand_score(terms, candidates_with_upper, &doc_lengths, top_k)
    }
}

impl WandSearcher {
    fn compute_idf(&self, df: u32) -> f32 {
        let n = self.num_docs as f32;
        let df = df as f32;
        ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
    }

    fn bm25_term_score(&self, tf: u32, idf: f32, doc_len: u32) -> f32 {
        let tf = tf as f32;
        let doc_len = doc_len as f32;
        let numerator = tf * (self.k1 + 1.0);
        let denominator = tf + self.k1 * (1.0 - self.b + self.b * doc_len / self.avgdl);
        idf * numerator / denominator
    }

    fn compute_candidates(&self, terms: &[TermInfo], top_k: usize) -> FxHashSet<u32> {
        if terms.is_empty() {
            return FxHashSet::default();
        }

        // Start with rarest term
        let mut candidates: FxHashSet<u32> = terms[0].postings.keys().copied().collect();

        // Early exit for single term or small candidate set
        if terms.len() == 1 || candidates.len() <= top_k * 5 {
            return candidates;
        }

        // Progressive intersection with early termination
        for term in terms.iter().skip(1) {
            let term_docs: FxHashSet<u32> = term.postings.keys().copied().collect();
            let intersection: FxHashSet<u32> =
                candidates.intersection(&term_docs).copied().collect();

            // Keep intersection if it has enough candidates
            if intersection.len() >= top_k * 2 {
                candidates = intersection;
            }

            // Stop early if we have a good candidate set
            if candidates.len() <= top_k * 5 {
                break;
            }
        }

        candidates
    }

    fn wand_score(
        &self,
        terms: Vec<TermInfo>,
        mut candidates: Vec<(f32, u32)>,
        doc_lengths: &FxHashMap<u32, u32>,
        top_k: usize,
    ) -> Vec<(u32, f32)> {
        candidates.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(Ordering::Equal));

        let mut heap: BinaryHeap<ScoredDoc> = BinaryHeap::with_capacity(top_k + 1);
        let mut threshold = 0.0f32;

        for (upper, doc_id) in candidates {
            if heap.len() >= top_k && upper <= threshold {
                break;
            }

            let doc_len = match doc_lengths.get(&doc_id) {
                Some(&len) => len,
                None => continue,
            };

            let score: f32 = terms
                .iter()
                .filter_map(|term| {
                    term.postings
                        .get(&doc_id)
                        .map(|&tf| self.bm25_term_score(tf, term.idf, doc_len))
                })
                .sum();

            if heap.len() < top_k {
                heap.push(ScoredDoc { doc_id, score });
                if heap.len() == top_k {
                    threshold = heap.peek().map(|d| d.score).unwrap_or(0.0);
                }
            } else if score > threshold {
                heap.pop();
                heap.push(ScoredDoc { doc_id, score });
                threshold = heap.peek().map(|d| d.score).unwrap_or(0.0);
            }
        }

        let mut results: Vec<_> = heap.into_iter().map(|d| (d.doc_id, d.score)).collect();
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
        results
    }
}
