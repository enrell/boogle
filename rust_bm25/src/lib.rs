use pyo3::prelude::*;
use pyo3::types::PyBytes;
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, BufWriter};

#[pyfunction]
fn analyze(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_ascii_alphabetic())
        .filter(|s| s.len() >= 2)
        .map(String::from)
        .collect()
}

fn encode_postings_internal(postings: &[(u32, u32)]) -> Vec<u8> {
    let mut sorted: Vec<_> = postings.to_vec();
    sorted.sort_by_key(|p| p.0);
    
    let mut result = Vec::with_capacity(sorted.len() * 4);
    let mut prev_doc_id = 0u32;
    
    for (doc_id, tf) in sorted {
        let delta = doc_id - prev_doc_id;
        prev_doc_id = doc_id;
        encode_varint(delta, &mut result);
        encode_varint(tf, &mut result);
    }
    result
}

fn decode_postings_internal(data: &[u8]) -> Vec<(u32, u32)> {
    let mut result = Vec::new();
    let mut pos = 0;
    let mut doc_id = 0u32;
    
    while pos < data.len() {
        let (delta, new_pos) = decode_varint(data, pos);
        pos = new_pos;
        let (tf, new_pos) = decode_varint(data, pos);
        pos = new_pos;
        doc_id += delta;
        result.push((doc_id, tf));
    }
    result
}

#[pyfunction]
fn encode_postings(py: Python<'_>, postings: Vec<(u32, u32)>) -> Py<PyBytes> {
    let result = encode_postings_internal(&postings);
    PyBytes::new_bound(py, &result).into()
}

#[pyfunction]
fn decode_postings(data: &[u8]) -> Vec<(u32, u32)> {
    decode_postings_internal(data)
}

fn encode_varint(mut value: u32, buf: &mut Vec<u8>) {
    while value >= 0x80 {
        buf.push((value as u8) | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
}

fn decode_varint(data: &[u8], mut pos: usize) -> (u32, usize) {
    let mut result = 0u32;
    let mut shift = 0;
    loop {
        let byte = data[pos];
        pos += 1;
        result |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    (result, pos)
}

#[derive(Serialize, Deserialize, Default)]
struct IndexData {
    k1: f32,
    b: f32,
    terms: FxHashMap<String, Vec<u8>>,
    term_df: FxHashMap<String, u32>,
    doc_lengths: Vec<u32>,
    doc_metadata: Vec<String>,
    num_docs: u32,
    avgdl: f32,
}

#[pyclass]
pub struct BM25Index {
    data: IndexData,
    pending: FxHashMap<String, Vec<(u32, u32)>>,
}

#[pymethods]
impl BM25Index {
    #[new]
    #[pyo3(signature = (k1=1.5, b=0.75))]
    fn new(k1: f32, b: f32) -> Self {
        Self {
            data: IndexData { k1, b, ..Default::default() },
            pending: FxHashMap::default(),
        }
    }

    fn add_document(&mut self, doc_id: u32, text: &str, metadata: &str) {
        let tokens = analyze(text);
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
            self.pending.entry(term.to_string()).or_default().push((doc_id, freq));
        }
        
        self.data.num_docs = self.data.num_docs.max(doc_id + 1);
    }

    fn finalize(&mut self) {
        let total: u64 = self.data.doc_lengths.iter().map(|&x| x as u64).sum();
        self.data.avgdl = if self.data.num_docs > 0 {
            total as f32 / self.data.num_docs as f32
        } else {
            0.0
        };
        
        for (term, postings) in self.pending.drain() {
            let df = postings.len() as u32;
            let encoded = encode_postings_internal(&postings);
            
            if let Some(existing) = self.data.terms.get_mut(&term) {
                let mut decoded = decode_postings_internal(existing);
                decoded.extend(decode_postings_internal(&encoded));
                *existing = encode_postings_internal(&decoded);
                *self.data.term_df.get_mut(&term).unwrap() += df;
            } else {
                self.data.terms.insert(term.clone(), encoded);
                self.data.term_df.insert(term, df);
            }
        }
    }

    fn search(&self, query: &str, top_k: usize) -> Vec<(u32, f32, String)> {
        let tokens = analyze(query);
        let mut candidates: FxHashMap<u32, f32> = FxHashMap::default();
        
        for token in &tokens {
            if let (Some(postings_blob), Some(&df)) = (self.data.terms.get(token), self.data.term_df.get(token)) {
                let idf = self.idf(df);
                let postings = decode_postings_internal(postings_blob);
                for (doc_id, tf) in postings {
                    let doc_len = self.data.doc_lengths[doc_id as usize] as f32;
                    let score = self.bm25_score(tf as f32, idf, doc_len);
                    *candidates.entry(doc_id).or_insert(0.0) += score;
                }
            }
        }
        
        let mut results: Vec<_> = candidates.into_iter().collect();
        results.par_sort_unstable_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        results.truncate(top_k);
        
        results.into_iter()
            .map(|(doc_id, score)| {
                let meta = self.data.doc_metadata.get(doc_id as usize).cloned().unwrap_or_default();
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
        let data: IndexData = bincode::deserialize_from(reader)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
        Ok(Self { data, pending: FxHashMap::default() })
    }

    #[getter]
    fn num_docs(&self) -> u32 { self.data.num_docs }
    
    #[getter]
    fn num_terms(&self) -> usize { self.data.terms.len() }
    
    #[getter]
    fn avgdl(&self) -> f32 { self.data.avgdl }
}

impl BM25Index {
    fn idf(&self, df: u32) -> f32 {
        let n = self.data.num_docs as f32;
        let df = df as f32;
        ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
    }

    fn bm25_score(&self, tf: f32, idf: f32, doc_len: f32) -> f32 {
        let k1 = self.data.k1;
        let b = self.data.b;
        let avgdl = self.data.avgdl;
        let numerator = tf * (k1 + 1.0);
        let denominator = tf + k1 * (1.0 - b + b * doc_len / avgdl);
        idf * numerator / denominator
    }
}

#[pymodule]
fn rust_bm25(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<BM25Index>()?;
    m.add_function(wrap_pyfunction!(analyze, m)?)?;
    m.add_function(wrap_pyfunction!(encode_postings, m)?)?;
    m.add_function(wrap_pyfunction!(decode_postings, m)?)?;
    Ok(())
}
