use crate::analysis::analyze;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Document {
    pub id: u32,
    pub content: String,
    pub metadata: String, // JSON string
    pub length: u32,      // Cached length for scoring
}

pub struct RamIndex {
    // Term -> List of (DocId, Frequency)
    pub inverted_index: FxHashMap<String, Vec<(u32, u32)>>,
    // DocId -> Document
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

        // Add to document store
        let doc = Document {
            id: doc_id,
            content: content.clone(),
            metadata,
            length: doc_length,
        };
        self.docs.insert(doc_id, doc);

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

        let mut scores: FxHashMap<u32, f32> = FxHashMap::default();

        let k1 = 1.2;
        let b = 0.75;
        let avgdl = if self.docs.is_empty() {
            0.0
        } else {
            self.total_length as f32 / self.docs.len() as f32
        };

        for token in tokens {
            if let Some(postings) = self.inverted_index.get(&token) {
                // Local IDF
                let doc_count = self.docs.len() as f32;
                let df = postings.len() as f32;
                // Avoid log(0) or negative
                let idf = ((doc_count - df + 0.5) / (df + 0.5) + 1.0).ln();

                for (doc_id, freq) in postings {
                    // Safe unwrap because inverted_index is built from docs
                    let doc = self.docs.get(doc_id).unwrap();
                    let doc_len = doc.length as f32;

                    let tf = *freq as f32;
                    let numerator = tf * (k1 + 1.0);
                    let denominator = tf + k1 * (1.0 - b + b * (doc_len / avgdl));
                    let score = idf * numerator / denominator;

                    *scores.entry(*doc_id).or_insert(0.0) += score;
                }
            }
        }

        let mut results: Vec<_> = scores.into_iter().collect();
        // Sort by score descending
        results.sort_unstable_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results
    }

    pub fn clear(&mut self) {
        self.inverted_index.clear();
        self.docs.clear();
        self.total_length = 0;
        // Keep next_doc_id as is to avoid collisions if we keep adding
    }
}
