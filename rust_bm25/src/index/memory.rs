use pyo3::prelude::*;
use pyo3::types::PyBytes;
use rkyv::{Archive, Deserialize, Serialize as RkyvSerialize};
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};

use crate::analysis::analyze;
use crate::codecs::{decode_postings_internal, encode_postings_internal};
use crate::document::parsers::{chunk_text, parse_file};
use rayon::prelude::*;
use serde_json::json;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};

#[pyfunction]
pub fn process_books_to_index(
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
    let doc_counter = AtomicU32::new(0);

    // Parallel processing with Rayon
    let results: Vec<_> = paths
        .into_par_iter()
        .zip(metadatas.into_par_iter())
        .map(|(path, base_meta)| {
            let text = match parse_file(&path) {
                Some(t) => t,
                None => return (Vec::new(), FxHashMap::default(), 0),
            };

            // Parse base metadata once using serde_json
            let meta_obj: serde_json::Value = serde_json::from_str(&base_meta).unwrap_or(json!({}));

            let title = meta_obj
                .get("title")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            let author = meta_obj
                .get("author")
                .and_then(|v| v.as_str())
                .unwrap_or_default();

            let title_tokens: Vec<String> = analyze(&format!("{} {}", title, author));

            let chunks = chunk_text(&text, chunk_size, overlap);

            let mut local_docs = Vec::with_capacity(chunks.len());
            let mut local_terms: FxHashMap<String, Vec<(u32, u32)>> = FxHashMap::default();
            let mut local_len = 0u64;

            for chunk in chunks {
                let tokens = analyze(&chunk);
                let doc_length = tokens.len() as u32;

                let doc_id = doc_counter.fetch_add(1, AtomicOrdering::SeqCst);

                // Clone the base object and add chunk-specific fields
                let mut chunk_meta = meta_obj.clone();
                chunk_meta["chunk_id"] = json!(doc_id);
                chunk_meta["title_tokens"] = json!(title_tokens);

                // Remove newlines to keep it safe for line-based storage if needed
                let meta_str = chunk_meta.to_string();

                local_docs.push((doc_id, doc_length, meta_str));
                local_len += doc_length as u64;

                let mut term_freqs: FxHashMap<&str, u32> = FxHashMap::default();
                for token in &tokens {
                    *term_freqs.entry(token.as_str()).or_insert(0) += 1;
                }

                for (term, freq) in term_freqs {
                    local_terms
                        .entry(term.to_string())
                        .or_default()
                        .push((doc_id, freq));
                }
            }

            (local_docs, local_terms, local_len)
        })
        .collect();

    // Merge results
    let mut all_docs = Vec::new();
    let mut all_terms: FxHashMap<String, Vec<(u32, u32)>> = FxHashMap::default();
    let mut total_length = 0u64;

    for (docs, terms, len) in results {
        all_docs.extend(docs);
        total_length += len;
        for (term, postings) in terms {
            all_terms.entry(term).or_default().extend(postings);
        }
    }

    let terms_result: Vec<_> = all_terms
        .into_iter()
        .map(|(term, postings)| {
            let df = postings.len() as u32;
            let encoded = encode_postings_internal(&postings);
            let py_bytes = PyBytes::new(py, &encoded).into();
            (term, df, py_bytes)
        })
        .collect();

    (all_docs, terms_result, total_length)
}

#[derive(Archive, RkyvSerialize, Deserialize, SerdeSerialize, SerdeDeserialize, Default)]
#[rkyv(derive(Debug))]
struct IndexData {
    k1: f32,
    b: f32,
    terms: std::collections::HashMap<String, Vec<u8>>,
    term_df: std::collections::HashMap<String, u32>,
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
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&self.data)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
        writer
            .write_all(&bytes)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
        Ok(())
    }

    #[staticmethod]
    fn load(path: &str) -> PyResult<Self> {
        let file = File::open(path)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
        let mut reader = BufReader::new(file);
        let mut bytes = Vec::new();
        reader
            .read_to_end(&mut bytes)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
        let archived = rkyv::access::<ArchivedIndexData, rkyv::rancor::Error>(&bytes)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
        let data: IndexData = rkyv::deserialize::<IndexData, rkyv::rancor::Error>(archived)
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

#[pyfunction]
pub fn process_batch(
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
    // Do all the heavy parallel work inside allow_threads (now detach) to release the GIL
    let (all_chunk_records, all_terms_raw, total_len, count) = py.detach(|| {
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
                    zstd::stream::encode_all(full_text.as_bytes(), 3).unwrap_or_default();
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
            let py_bytes = PyBytes::new(py, &encoded).into();
            (term, df, py_bytes)
        })
        .collect();

    (all_chunk_records, terms_result, total_len, count)
}
