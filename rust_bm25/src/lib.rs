use pyo3::prelude::*;
use pyo3::types::PyBytes;
use rayon::prelude::*;
use regex::Regex;
use rustc_hash::FxHashMap;
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read};
use std::sync::Mutex;
use zip::ZipArchive;

#[pyfunction]
fn analyze(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_ascii_alphabetic())
        .filter(|s| s.len() >= 2 && s.len() <= 30)
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
        if pos >= data.len() {
            break;
        }
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

#[pyfunction]
fn merge_postings(py: Python<'_>, a: &[u8], b: &[u8]) -> Py<PyBytes> {
    let mut postings_a = decode_postings_internal(a);
    let postings_b = decode_postings_internal(b);
    postings_a.extend(postings_b);
    let result = encode_postings_internal(&postings_a);
    PyBytes::new_bound(py, &result).into()
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

fn extract_text_from_html(html: &str) -> String {
    let document = Html::parse_document(html);
    let selector = Selector::parse("body").unwrap();
    let mut text = String::new();

    if let Some(body) = document.select(&selector).next() {
        for node in body.text() {
            text.push_str(node);
            text.push(' ');
        }
    } else {
        for node in document.root_element().text() {
            text.push_str(node);
            text.push(' ');
        }
    }

    let whitespace = Regex::new(r"\s+").unwrap();
    whitespace.replace_all(&text, " ").trim().to_string()
}

fn parse_epub_internal(path: &str) -> Option<String> {
    let file = File::open(path).ok()?;
    let reader = BufReader::new(file);
    let mut archive = ZipArchive::new(reader).ok()?;

    let mut texts = Vec::new();

    for i in 0..archive.len() {
        let mut file = archive.by_index(i).ok()?;
        let name = file.name().to_lowercase();

        if name.ends_with(".html") || name.ends_with(".xhtml") || name.ends_with(".htm") {
            let mut content = String::new();
            file.read_to_string(&mut content).ok()?;
            let text = extract_text_from_html(&content);
            if !text.is_empty() {
                texts.push(text);
            }
        }
    }

    Some(texts.join(" "))
}

#[pyfunction]
fn parse_epub(path: &str) -> Option<String> {
    parse_epub_internal(path)
}

#[pyfunction]
fn chunk_text(text: &str, chunk_size: usize, overlap: usize) -> Vec<String> {
    if text.len() <= chunk_size {
        return if text.is_empty() {
            vec![]
        } else {
            vec![text.to_string()]
        };
    }

    let mut chunks = Vec::new();
    let bytes = text.as_bytes();
    let mut start = 0;

    while start < bytes.len() {
        let mut end = (start + chunk_size).min(bytes.len());

        if end < bytes.len() {
            while end > start && bytes[end] != b' ' {
                end -= 1;
            }
            if end == start {
                end = (start + chunk_size).min(bytes.len());
            }
        }

        if let Ok(chunk) = std::str::from_utf8(&bytes[start..end]) {
            let trimmed = chunk.trim();
            if !trimmed.is_empty() {
                chunks.push(trimmed.to_string());
            }
        }

        start = if end > overlap { end - overlap } else { end };
        if start >= bytes.len() || end == bytes.len() {
            break;
        }
    }

    chunks
}

#[pyclass]
pub struct BatchIndexer {
    terms: FxHashMap<String, Vec<(u32, u32)>>,
    docs: Vec<(u32, u32, String)>,
    total_length: u64,
    next_doc_id: u32,
    memory_bytes: usize,
    max_memory_bytes: usize,
}

#[pymethods]
impl BatchIndexer {
    #[new]
    #[pyo3(signature = (max_memory_mb=256))]
    fn new(max_memory_mb: usize) -> Self {
        Self {
            terms: FxHashMap::default(),
            docs: Vec::new(),
            total_length: 0,
            next_doc_id: 0,
            memory_bytes: 0,
            max_memory_bytes: max_memory_mb * 1024 * 1024,
        }
    }

    fn add_document(&mut self, text: &str, metadata: &str) -> u32 {
        let doc_id = self.next_doc_id;
        self.next_doc_id += 1;

        let tokens = analyze(text);
        let doc_length = tokens.len() as u32;

        self.docs.push((doc_id, doc_length, metadata.to_string()));
        self.total_length += doc_length as u64;
        self.memory_bytes += metadata.len() + 12;

        let mut term_freqs: FxHashMap<&str, u32> = FxHashMap::default();
        for token in &tokens {
            *term_freqs.entry(token.as_str()).or_insert(0) += 1;
        }

        for (term, freq) in term_freqs {
            let is_new = !self.terms.contains_key(term);
            let postings = self.terms.entry(term.to_string()).or_default();
            postings.push((doc_id, freq));
            if is_new {
                self.memory_bytes += term.len() + 24;
            }
            self.memory_bytes += 8;
        }

        doc_id
    }

    fn should_flush(&self) -> bool {
        self.memory_bytes >= self.max_memory_bytes
    }

    fn get_flush_data(&mut self, py: Python<'_>) -> (Vec<(u32, u32, String)>, Vec<(String, u32, Py<PyBytes>)>) {
        let docs = std::mem::take(&mut self.docs);
        let terms: Vec<_> = self.terms.drain()
            .map(|(term, postings)| {
                let df = postings.len() as u32;
                let encoded = encode_postings_internal(&postings);
                let py_bytes = PyBytes::new_bound(py, &encoded).into();
                (term, df, py_bytes)
            })
            .collect();
        
        self.memory_bytes = 0;
        (docs, terms)
    }

    fn num_docs(&self) -> u32 {
        self.next_doc_id
    }

    fn num_pending_docs(&self) -> usize {
        self.docs.len()
    }

    fn num_terms(&self) -> usize {
        self.terms.len()
    }

    fn memory_mb(&self) -> f64 {
        self.memory_bytes as f64 / (1024.0 * 1024.0)
    }

    fn total_length(&self) -> u64 {
        self.total_length
    }

    fn set_next_doc_id(&mut self, doc_id: u32) {
        self.next_doc_id = doc_id;
    }
}

#[pyfunction]
fn process_epubs_to_index(
    py: Python<'_>,
    paths: Vec<String>,
    metadatas: Vec<String>,
    chunk_size: usize,
    overlap: usize,
) -> (Vec<(u32, u32, String)>, Vec<(String, u32, Py<PyBytes>)>, u64) {
    let terms: Mutex<FxHashMap<String, Vec<(u32, u32)>>> = Mutex::new(FxHashMap::default());
    let docs: Mutex<Vec<(u32, u32, String)>> = Mutex::new(Vec::new());
    let total_length: Mutex<u64> = Mutex::new(0);
    let doc_counter: Mutex<u32> = Mutex::new(0);

    paths.into_par_iter().zip(metadatas.into_par_iter()).for_each(|(path, base_meta)| {
        let text = match parse_epub_internal(&path) {
            Some(t) => t,
            None => return,
        };

        let chunks = chunk_text(&text, chunk_size, overlap);
        
        for chunk in chunks {
            let tokens = analyze(&chunk);
            let doc_length = tokens.len() as u32;

            let doc_id = {
                let mut counter = doc_counter.lock().unwrap();
                let id = *counter;
                *counter += 1;
                id
            };

            let meta = format!("{}\"chunk_id\":{}}}", &base_meta[..base_meta.len()-1], doc_id);

            {
                let mut docs_lock = docs.lock().unwrap();
                docs_lock.push((doc_id, doc_length, meta));
            }

            {
                let mut total = total_length.lock().unwrap();
                *total += doc_length as u64;
            }

            let mut term_freqs: FxHashMap<&str, u32> = FxHashMap::default();
            for token in &tokens {
                *term_freqs.entry(token.as_str()).or_insert(0) += 1;
            }

            {
                let mut terms_lock = terms.lock().unwrap();
                for (term, freq) in term_freqs {
                    terms_lock.entry(term.to_string()).or_default().push((doc_id, freq));
                }
            }
        }
    });

    let docs_result = docs.into_inner().unwrap();
    let total = *total_length.lock().unwrap();
    
    let terms_result: Vec<_> = terms.into_inner().unwrap()
        .into_iter()
        .map(|(term, postings)| {
            let df = postings.len() as u32;
            let encoded = encode_postings_internal(&postings);
            let py_bytes = PyBytes::new_bound(py, &encoded).into();
            (term, df, py_bytes)
        })
        .collect();

    (docs_result, terms_result, total)
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
            data: IndexData {
                k1,
                b,
                ..Default::default()
            },
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
            self.pending
                .entry(term.to_string())
                .or_default()
                .push((doc_id, freq));
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
            if let (Some(postings_blob), Some(&df)) =
                (self.data.terms.get(token), self.data.term_df.get(token))
            {
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

        results
            .into_iter()
            .map(|(doc_id, score)| {
                let meta = self
                    .data
                    .doc_metadata
                    .get(doc_id as usize)
                    .cloned()
                    .unwrap_or_default();
                (doc_id, score, meta)
            })
            .collect()
    }

    fn save(&self, path: &str) -> PyResult<()> {
        let file = File::create(path)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
        let writer = BufWriter::new(file);
        bincode::serialize_into(writer, &self.data)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
        Ok(())
    }

    #[staticmethod]
    fn load(path: &str) -> PyResult<Self> {
        let file = File::open(path)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
        let reader = BufReader::new(file);
        let data: IndexData = bincode::deserialize_from(reader)
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

    #[getter]
    fn num_terms(&self) -> usize {
        self.data.terms.len()
    }

    #[getter]
    fn avgdl(&self) -> f32 {
        self.data.avgdl
    }
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
    m.add_class::<BatchIndexer>()?;
    m.add_function(wrap_pyfunction!(analyze, m)?)?;
    m.add_function(wrap_pyfunction!(encode_postings, m)?)?;
    m.add_function(wrap_pyfunction!(decode_postings, m)?)?;
    m.add_function(wrap_pyfunction!(merge_postings, m)?)?;
    m.add_function(wrap_pyfunction!(parse_epub, m)?)?;
    m.add_function(wrap_pyfunction!(chunk_text, m)?)?;
    m.add_function(wrap_pyfunction!(process_epubs_to_index, m)?)?;
    Ok(())
}
