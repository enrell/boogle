use pyo3::prelude::*;
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, BufWriter};

#[derive(Serialize, Deserialize, Default)]
struct BM25Data {
    k1: f32,
    b: f32,
    inverted_index: FxHashMap<String, Vec<(u32, u32)>>,
    doc_lengths: Vec<u32>,
    doc_metadata: Vec<String>,
    avg_doc_length: f32,
    num_docs: u32,
}

#[pyclass]
pub struct BM25Index {
    data: BM25Data,
    pending: FxHashMap<String, Vec<(u32, u32)>>,
}

#[pymethods]
impl BM25Index {
    #[new]
    #[pyo3(signature = (k1=1.5, b=0.75))]
    fn new(k1: f32, b: f32) -> Self {
        Self {
            data: BM25Data {
                k1,
                b,
                ..Default::default()
            },
            pending: FxHashMap::default(),
        }
    }

    fn add_document(&mut self, doc_id: u32, text: &str, metadata: &str) {
        let tokens = tokenize(text);
        let doc_length = tokens.len() as u32;
        
        while self.data.doc_lengths.len() <= doc_id as usize {
            self.data.doc_lengths.push(0);
            self.data.doc_metadata.push(String::new());
        }
        self.data.doc_lengths[doc_id as usize] = doc_length;
        self.data.doc_metadata[doc_id as usize] = metadata.to_string();
        
        let mut term_freqs: FxHashMap<&str, u32> = FxHashMap::default();
        for token in &tokens {
            *term_freqs.entry(token.as_str()).or_insert(0) += 1;
        }
        
        for (term, freq) in term_freqs {
            self.pending
                .entry(term.to_string())
                .or_default()
                .push((doc_id, freq));
        }
        
        self.data.num_docs = self.data.num_docs.max(doc_id + 1);
    }

    fn finalize(&mut self) {
        let total: u64 = self.data.doc_lengths.iter().map(|&x| x as u64).sum();
        self.data.avg_doc_length = if self.data.num_docs > 0 {
            total as f32 / self.data.num_docs as f32
        } else {
            0.0
        };
        
        for (term, postings) in self.pending.drain() {
            self.data
                .inverted_index
                .entry(term)
                .or_default()
                .extend(postings);
        }
    }

    fn search(&self, query: &str, top_k: usize) -> Vec<(u32, f32, String)> {
        let tokens = tokenize(query);
        let mut candidates: FxHashMap<u32, f32> = FxHashMap::default();
        
        for token in &tokens {
            if let Some(postings) = self.data.inverted_index.get(token) {
                let idf = self.idf(postings.len());
                for &(doc_id, tf) in postings {
                    let doc_len = self.data.doc_lengths[doc_id as usize] as f32;
                    let score = self.bm25_score(tf as f32, idf, doc_len);
                    *candidates.entry(doc_id).or_insert(0.0) += score;
                }
            }
        }
        
        let mut results: Vec<_> = candidates.into_iter().collect();
        results.par_sort_unstable_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        results.truncate(top_k);
        
        results
            .into_iter()
            .map(|(doc_id, score)| {
                let meta = self.data.doc_metadata.get(doc_id as usize)
                    .cloned()
                    .unwrap_or_default();
                (doc_id, score, meta)
            })
            .collect()
    }

    fn save(&self, path: &str) -> PyResult<()> {
        let file = File::create(path).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
        let writer = BufWriter::new(file);
        bincode::serialize_into(writer, &self.data)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
        Ok(())
    }

    #[staticmethod]
    fn load(path: &str) -> PyResult<Self> {
        let file = File::open(path).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
        let reader = BufReader::new(file);
        let data: BM25Data = bincode::deserialize_from(reader)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
        Ok(Self {
            data,
            pending: FxHashMap::default(),
        })
    }

    #[getter]
    fn num_docs(&self) -> u32 {
        self.data.num_docs
    }
}

impl BM25Index {
    fn idf(&self, df: usize) -> f32 {
        let n = self.data.num_docs as f32;
        let df = df as f32;
        ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
    }

    fn bm25_score(&self, tf: f32, idf: f32, doc_len: f32) -> f32 {
        let k1 = self.data.k1;
        let b = self.data.b;
        let avg_dl = self.data.avg_doc_length;
        let numerator = tf * (k1 + 1.0);
        let denominator = tf + k1 * (1.0 - b + b * doc_len / avg_dl);
        idf * numerator / denominator
    }
}

fn tokenize(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_ascii_alphabetic())
        .filter(|s| s.len() >= 2)
        .map(String::from)
        .collect()
}

#[pymodule]
fn rust_bm25(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<BM25Index>()?;
    Ok(())
}
