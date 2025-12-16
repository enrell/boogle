use pyo3::prelude::*;
use pyo3::types::PyBytes;
use rayon::prelude::*;
use regex::Regex;
use rustc_hash::{FxHashMap, FxHashSet};
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read};
use std::sync::Mutex;
use zip::ZipArchive;

#[pyfunction]
fn analyze(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_alphabetic())
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

fn extract_json_field(json: &str, field: &str) -> Option<String> {
    // Try with space (Python default) and without
    let patterns = [format!("\"{}\": \"", field), format!("\"{}\":\"", field)];

    for pattern in &patterns {
        if let Some(start_idx) = json.find(pattern) {
            let start = start_idx + pattern.len();
            let rest = &json[start..];
            if let Some(end) = rest.find('"') {
                return Some(rest[..end].to_string());
            }
        }
    }
    None
}

fn should_skip_epub_file(name: &str) -> bool {
    let skip_patterns = [
        "toc",
        "nav",
        "cover",
        "license",
        "gutenberg",
        "copyright",
        "colophon",
    ];
    let lower = name.to_lowercase();
    skip_patterns.iter().any(|p| lower.contains(p))
}

fn parse_epub_internal(path: &str) -> Option<String> {
    let file = File::open(path).ok()?;
    let reader = BufReader::new(file);
    let mut archive = ZipArchive::new(reader).ok()?;

    let mut texts = Vec::new();

    for i in 0..archive.len() {
        let mut file = archive.by_index(i).ok()?;
        let name = file.name().to_lowercase();

        if (name.ends_with(".html") || name.ends_with(".xhtml") || name.ends_with(".htm"))
            && !should_skip_epub_file(&name)
        {
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
fn parse_pdf(path: &str) -> Option<String> {
    let bytes = std::fs::read(path).ok()?;
    let text = pdf_extract::extract_text_from_mem(&bytes).ok()?;
    let whitespace = Regex::new(r"\s+").unwrap();
    Some(whitespace.replace_all(&text, " ").trim().to_string())
}

#[pyfunction]
fn parse_txt(path: &str) -> Option<String> {
    let text = std::fs::read_to_string(path).ok()?;
    let whitespace = Regex::new(r"\s+").unwrap();
    Some(whitespace.replace_all(&text, " ").trim().to_string())
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

fn parse_file(path: &str) -> Option<String> {
    if path.ends_with(".epub") {
        parse_epub_internal(path)
    } else if path.ends_with(".pdf") {
        let bytes = std::fs::read(path).ok()?;
        let text = pdf_extract::extract_text_from_mem(&bytes).ok()?;
        let whitespace = Regex::new(r"\s+").unwrap();
        Some(whitespace.replace_all(&text, " ").trim().to_string())
    } else if path.ends_with(".txt") {
        let text = std::fs::read_to_string(path).ok()?;
        let whitespace = Regex::new(r"\s+").unwrap();
        Some(whitespace.replace_all(&text, " ").trim().to_string())
    } else {
        None
    }
}

#[pyfunction]
fn process_books_to_index(
    py: Python<'_>,
    paths: Vec<String>,
    metadatas: Vec<String>,
    chunk_size: usize,
    overlap: usize,
) -> (
    Vec<(u32, u32, String)>,
    Vec<(String, u32, Py<PyBytes>)>,
    u64,
) {
    let terms: Mutex<FxHashMap<String, Vec<(u32, u32)>>> = Mutex::new(FxHashMap::default());
    let docs: Mutex<Vec<(u32, u32, String)>> = Mutex::new(Vec::new());
    let total_length: Mutex<u64> = Mutex::new(0);
    let doc_counter: Mutex<u32> = Mutex::new(0);

    paths
        .into_par_iter()
        .zip(metadatas.into_par_iter())
        .for_each(|(path, base_meta)| {
            let text = match parse_file(&path) {
                Some(t) => t,
                None => return,
            };

            let title = extract_json_field(&base_meta, "title").unwrap_or_default();
            let author = extract_json_field(&base_meta, "author").unwrap_or_default();
            let title_tokens: Vec<String> = analyze(&format!("{} {}", title, author));
            let title_tokens_json: String = title_tokens
                .iter()
                .map(|t| format!("\"{}\"", t))
                .collect::<Vec<_>>()
                .join(",");

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

                let meta = format!(
                    "{},\"chunk_id\":{},\"title_tokens\":[{}]}}",
                    &base_meta[..base_meta.len() - 1],
                    doc_id,
                    title_tokens_json
                );

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
                        terms_lock
                            .entry(term.to_string())
                            .or_default()
                            .push((doc_id, freq));
                    }
                }
            }
        });

    let docs_result = docs.into_inner().unwrap();
    let total = *total_length.lock().unwrap();

    let terms_result: Vec<_> = terms
        .into_inner()
        .unwrap()
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

#[derive(Serialize, Deserialize, Default, bincode::Encode, bincode::Decode)]
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

    fn save(&self, path: &str) -> PyResult<()> {
        let file = File::create(path)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
        let mut writer = BufWriter::new(file);
        bincode::encode_into_std_write(&self.data, &mut writer, bincode::config::standard())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
        Ok(())
    }

    #[staticmethod]
    fn load(path: &str) -> PyResult<Self> {
        let file = File::open(path)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
        let mut reader = BufReader::new(file);
        let data: IndexData =
            bincode::decode_from_std_read(&mut reader, bincode::config::standard())
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

use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};

#[pyfunction]
fn process_batch(
    py: Python<'_>,
    paths: Vec<String>,
    book_ids: Vec<String>,
    chunk_size: usize,
    overlap: usize,
    start_doc_id: u32,
    chunks_dir: String,
    stopwords: Vec<String>,
) -> (
    Vec<(u32, String)>,
    Vec<(String, u32, Py<PyBytes>)>,
    u64,
    u32,
) {
    // Do all the heavy parallel work inside allow_threads to release the GIL
    let (all_chunk_records, all_terms_raw, total_len, count) = py.allow_threads(|| {
        let stopwords_set: FxHashSet<String> = stopwords.into_iter().collect();
        let next_doc_id = AtomicU32::new(start_doc_id);
        let chunks_dir = Path::new(&chunks_dir);

        let results: Vec<_> = paths
            .into_par_iter()
            .zip(book_ids.into_par_iter())
            .map(|(path, book_id)| {
                let text = match parse_file(&path) {
                    Some(t) => t,
                    None => return None,
                };

                let chunks = chunk_text(&text, chunk_size, overlap);
                if chunks.is_empty() {
                    return None;
                }

                // Save chunks to zstd
                let shard_name = if book_id.len() < 2 {
                    format!("{:0>2}", book_id)
                } else {
                    book_id[..2].to_string()
                };

                let shard_dir = chunks_dir.join(&shard_name);
                std::fs::create_dir_all(&shard_dir).ok();
                let chunk_path = shard_dir.join(format!("{}.zst", book_id));

                let full_text = chunks.join("\n");
                let compressed =
                    zstd::stream::encode_all(full_text.as_bytes(), 0).unwrap_or_default();
                std::fs::write(chunk_path, compressed).ok();

                let mut local_chunk_records = Vec::with_capacity(chunks.len());
                let mut local_terms: FxHashMap<String, Vec<(u32, u32)>> = FxHashMap::default();
                let mut local_len = 0u64;

                for chunk in chunks {
                    let doc_id = next_doc_id.fetch_add(1, AtomicOrdering::SeqCst);
                    local_chunk_records.push((doc_id, book_id.clone()));

                    let tokens = analyze(&chunk);
                    local_len += tokens.len() as u64;

                    let mut freq_map: FxHashMap<&str, u32> = FxHashMap::default();
                    for token in &tokens {
                        if !stopwords_set.contains(token) {
                            *freq_map.entry(token).or_insert(0) += 1;
                        }
                    }

                    for (term, freq) in freq_map {
                        local_terms
                            .entry(term.to_string())
                            .or_default()
                            .push((doc_id, freq));
                    }
                }

                Some((local_chunk_records, local_terms, local_len))
            })
            .collect();

        let mut all_chunk_records = Vec::new();
        let mut all_terms: FxHashMap<String, Vec<(u32, u32)>> = FxHashMap::default();
        let mut total_len = 0u64;

        for res in results {
            if let Some((recs, terms, len)) = res {
                all_chunk_records.extend(recs);
                total_len += len;
                for (term, postings) in terms {
                    all_terms.entry(term).or_default().extend(postings);
                }
            }
        }

        let end_doc_id = next_doc_id.load(AtomicOrdering::SeqCst);
        let count = end_doc_id - start_doc_id;

        // Convert terms to raw bytes (Vec<u8>) instead of PyBytes inside the closure
        let all_terms_raw: Vec<(String, u32, Vec<u8>)> = all_terms
            .into_iter()
            .map(|(term, postings)| {
                let df = postings.len() as u32;
                let encoded = encode_postings_internal(&postings);
                (term, df, encoded)
            })
            .collect();

        (all_chunk_records, all_terms_raw, total_len, count)
    });

    // Convert raw bytes to PyBytes (requires GIL, done outside allow_threads)
    let terms_result: Vec<_> = all_terms_raw
        .into_iter()
        .map(|(term, df, encoded)| {
            let py_bytes = PyBytes::new_bound(py, &encoded).into();
            (term, df, py_bytes)
        })
        .collect();

    (all_chunk_records, terms_result, total_len, count)
}

/// Calculate MD5 hashes for multiple files in parallel
#[pyfunction]
fn file_hashes_batch(py: Python<'_>, paths: Vec<String>) -> Vec<(String, String)> {
    py.allow_threads(|| {
        paths
            .into_par_iter()
            .filter_map(|path| {
                let data = std::fs::read(&path).ok()?;
                let hash = format!("{:x}", md5::compute(&data));
                Some((path, hash))
            })
            .collect()
    })
}

#[pymodule]
fn rust_bm25(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<BM25Index>()?;
    m.add_class::<WandSearcher>()?;
    m.add_function(wrap_pyfunction!(analyze, m)?)?;
    m.add_function(wrap_pyfunction!(encode_postings, m)?)?;
    m.add_function(wrap_pyfunction!(decode_postings, m)?)?;
    m.add_function(wrap_pyfunction!(merge_postings, m)?)?;
    m.add_function(wrap_pyfunction!(parse_epub, m)?)?;
    m.add_function(wrap_pyfunction!(parse_pdf, m)?)?;
    m.add_function(wrap_pyfunction!(parse_txt, m)?)?;
    m.add_function(wrap_pyfunction!(chunk_text, m)?)?;
    m.add_function(wrap_pyfunction!(process_books_to_index, m)?)?;
    m.add_function(wrap_pyfunction!(process_batch, m)?)?;
    m.add_function(wrap_pyfunction!(file_hashes_batch, m)?)?;
    Ok(())
}
