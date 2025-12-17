use crossbeam_channel::bounded;
use dashmap::DashMap;
use fst::Map as FstMap;
use memmap2::Mmap;
use pyo3::prelude::*;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::thread;

use crate::analysis::analyze;
use crate::parsers::{chunk_text, parse_file};

#[derive(serde::Serialize, serde::Deserialize)]
struct SegmentMeta {
    num_docs: u32,
    base_doc_id: u32,
    total_length: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct IndexMeta {
    segments: Vec<String>,
    total_docs: u32,
    avgdl: f32,
}

struct ProcessedDoc {
    book_id: String,
    chunks: Vec<(u32, FxHashMap<String, u32>)>,
}

struct BatchData {
    segment_id: usize,
    segment_dir: PathBuf,
    docs: Vec<ProcessedDoc>,
    base_doc_id: u32,
}

fn write_segment(data: BatchData) -> std::io::Result<SegmentMeta> {
    fs::create_dir_all(&data.segment_dir)?;

    let total_chunks: usize = data.docs.iter().map(|d| d.chunks.len()).sum();
    let mut book_ids: Vec<String> = Vec::with_capacity(data.docs.len());
    let mut chunk_to_book: Vec<u16> = Vec::with_capacity(total_chunks);
    let mut doc_lengths: Vec<u32> = Vec::with_capacity(total_chunks);
    let mut chunk_freq_maps: Vec<(u32, FxHashMap<String, u32>)> = Vec::with_capacity(total_chunks);
    let mut total_length: u64 = 0;

    for doc in data.docs {
        let book_idx = book_ids.len() as u16;
        book_ids.push(doc.book_id);

        for (doc_length, freq_map) in doc.chunks {
            let doc_id = data.base_doc_id + chunk_to_book.len() as u32;
            chunk_to_book.push(book_idx);
            doc_lengths.push(doc_length);
            total_length += doc_length as u64;
            chunk_freq_maps.push((doc_id, freq_map));
        }
    }

    let terms: DashMap<String, Vec<(u32, u32)>> = DashMap::with_capacity(500_000);

    chunk_freq_maps
        .into_par_iter()
        .for_each(|(doc_id, freq_map)| {
            for (term, freq) in freq_map {
                terms.entry(term).or_default().push((doc_id, freq));
            }
        });

    let mut sorted_terms: Vec<(String, Vec<(u32, u32)>)> = terms
        .into_iter()
        .filter(|(_, postings)| postings.len() >= 2)
        .collect();
    sorted_terms.par_sort_unstable_by(|a, b| a.0.cmp(&b.0));

    let encoded_postings: Vec<(Vec<u8>, Vec<u8>)> = sorted_terms
        .par_iter()
        .map(|(_, postings)| crate::encoding::encode_postings_separated(postings))
        .collect();

    let mut term_offsets: Vec<(&str, u64)> = Vec::with_capacity(sorted_terms.len());
    // 28 bytes per term: offset_doc(8) + len_doc(4) + offset_freq(8) + len_freq(4) + doc_count(4)
    let mut offsets_data: Vec<u8> = Vec::with_capacity(sorted_terms.len() * 28);
    let mut current_offset_doc: u64 = 0;
    let mut current_offset_freq: u64 = 0;

    for (idx, (term, original_postings)) in sorted_terms.iter().enumerate() {
        let (docs_data, freqs_data) = &encoded_postings[idx];
        let len_doc = docs_data.len() as u32;
        let len_freq = freqs_data.len() as u32;
        let doc_count = original_postings.len() as u32; // Store doc_count

        offsets_data.extend_from_slice(&current_offset_doc.to_le_bytes()); // 0-8
        offsets_data.extend_from_slice(&len_doc.to_le_bytes()); // 8-12
        offsets_data.extend_from_slice(&current_offset_freq.to_le_bytes()); // 12-20
        offsets_data.extend_from_slice(&len_freq.to_le_bytes()); // 20-24
        offsets_data.extend_from_slice(&doc_count.to_le_bytes()); // 24-28

        term_offsets.push((term.as_str(), idx as u64));

        current_offset_doc += len_doc as u64;
        current_offset_freq += len_freq as u64;
    }

    let total_docs_size: usize = encoded_postings.iter().map(|(d, _)| d.len()).sum();
    let total_freqs_size: usize = encoded_postings.iter().map(|(_, f)| f.len()).sum();

    let mut postings_docs_blob: Vec<u8> = Vec::with_capacity(total_docs_size);
    let mut postings_freqs_blob: Vec<u8> = Vec::with_capacity(total_freqs_size);

    for (d, f) in &encoded_postings {
        postings_docs_blob.extend_from_slice(d);
        postings_freqs_blob.extend_from_slice(f);
    }

    let fst_map = FstMap::from_iter(term_offsets.into_iter())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    let fst_bytes = fst_map.as_fst().as_bytes().to_vec();

    let mut chunks_data: Vec<u8> = Vec::with_capacity(book_ids.iter().map(|s| s.len()).sum());
    let mut chunks_offsets: Vec<u32> = Vec::with_capacity(chunk_to_book.len() + 1);
    for &book_idx in &chunk_to_book {
        chunks_offsets.push(chunks_data.len() as u32);
        chunks_data.extend_from_slice(book_ids[book_idx as usize].as_bytes());
    }
    chunks_offsets.push(chunks_data.len() as u32);

    let mut chunks_blob: Vec<u8> =
        Vec::with_capacity((chunks_offsets.len() * 4) + chunks_data.len());
    for offset in &chunks_offsets {
        chunks_blob.extend_from_slice(&offset.to_le_bytes());
    }
    chunks_blob.extend_from_slice(&chunks_data);

    let mut lengths_blob: Vec<u8> = Vec::with_capacity(doc_lengths.len() * 4);
    for len in &doc_lengths {
        lengths_blob.extend_from_slice(&len.to_le_bytes());
    }

    let segment_dir = &data.segment_dir;
    fs::write(segment_dir.join("postings_docs.bin"), &postings_docs_blob)?;
    fs::write(segment_dir.join("postings_freqs.bin"), &postings_freqs_blob)?;

    fs::write(segment_dir.join("terms.fst"), &fst_bytes)?;
    fs::write(segment_dir.join("offsets.bin"), &offsets_data)?;
    fs::write(segment_dir.join("chunks.bin"), &chunks_blob)?;
    fs::write(segment_dir.join("doc_lengths.bin"), &lengths_blob)?;

    let meta = SegmentMeta {
        num_docs: chunk_to_book.len() as u32,
        base_doc_id: data.base_doc_id,
        total_length,
    };
    let meta_json = serde_json::to_string(&meta)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    fs::write(data.segment_dir.join("meta.json"), meta_json)?;

    Ok(meta)
}

struct SegmentReader {
    terms_fst: FstMap<Mmap>,
    offsets_mmap: Mmap,
    postings_docs_mmap: Mmap,
    postings_freqs_mmap: Mmap,
    chunks_mmap: Mmap,
    doc_lengths_mmap: Mmap,
    base_doc_id: u32,
    num_docs: u32,
}

impl SegmentReader {
    fn open(segment_dir: &Path) -> std::io::Result<Self> {
        let terms_file = File::open(segment_dir.join("terms.fst"))?;
        let terms_mmap = unsafe { Mmap::map(&terms_file)? };
        let terms_fst = FstMap::new(terms_mmap)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let offsets_file = File::open(segment_dir.join("offsets.bin"))?;
        let offsets_mmap = unsafe { Mmap::map(&offsets_file)? };

        let postings_docs_file = File::open(segment_dir.join("postings_docs.bin"))?;
        let postings_docs_mmap = unsafe { Mmap::map(&postings_docs_file)? };

        let postings_freqs_file = File::open(segment_dir.join("postings_freqs.bin"))?;
        let postings_freqs_mmap = unsafe { Mmap::map(&postings_freqs_file)? };

        let chunks_file = File::open(segment_dir.join("chunks.bin"))?;
        let chunks_mmap = unsafe { Mmap::map(&chunks_file)? };

        let lengths_file = File::open(segment_dir.join("doc_lengths.bin"))?;
        let doc_lengths_mmap = unsafe { Mmap::map(&lengths_file)? };

        let meta_str = fs::read_to_string(segment_dir.join("meta.json"))?;
        let meta: SegmentMeta = serde_json::from_str(&meta_str)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        Ok(Self {
            terms_fst,
            offsets_mmap,
            postings_docs_mmap,
            postings_freqs_mmap,
            chunks_mmap,
            doc_lengths_mmap,
            base_doc_id: meta.base_doc_id,
            num_docs: meta.num_docs,
        })
    }

    fn get_postings(&self, term: &str) -> Option<Vec<(u32, u32)>> {
        let term_idx = self.terms_fst.get(term)?;
        // 28 bytes per term: offset_doc(8) + len_doc(4) + offset_freq(8) + len_freq(4) + doc_count(4)
        let offset_pos = (term_idx as usize) * 28;

        if offset_pos + 28 > self.offsets_mmap.len() {
            return None;
        }

        let doc_offset = u64::from_le_bytes(
            self.offsets_mmap[offset_pos..offset_pos + 8]
                .try_into()
                .ok()?,
        );
        let doc_len = u32::from_le_bytes(
            self.offsets_mmap[offset_pos + 8..offset_pos + 12]
                .try_into()
                .ok()?,
        );
        let freq_offset = u64::from_le_bytes(
            self.offsets_mmap[offset_pos + 12..offset_pos + 20]
                .try_into()
                .ok()?,
        );
        let freq_len = u32::from_le_bytes(
            self.offsets_mmap[offset_pos + 20..offset_pos + 24]
                .try_into()
                .ok()?,
        );
        let doc_count = u32::from_le_bytes(
            self.offsets_mmap[offset_pos + 24..offset_pos + 28]
                .try_into()
                .ok()?,
        );

        let doc_end = (doc_offset + doc_len as u64) as usize;
        let freq_end = (freq_offset + freq_len as u64) as usize;

        if doc_end > self.postings_docs_mmap.len() || freq_end > self.postings_freqs_mmap.len() {
            return None;
        }

        Some(crate::encoding::decode_postings_separated(
            &self.postings_docs_mmap[doc_offset as usize..doc_end],
            &self.postings_freqs_mmap[freq_offset as usize..freq_end],
            doc_count as usize,
        ))
    }

    fn get_doc_length(&self, global_doc_id: u32) -> Option<u32> {
        let local_id = global_doc_id.checked_sub(self.base_doc_id)?;
        if local_id >= self.num_docs {
            return None;
        }
        let pos = (local_id as usize) * 4;
        if pos + 4 > self.doc_lengths_mmap.len() {
            return None;
        }
        Some(u32::from_le_bytes(
            self.doc_lengths_mmap[pos..pos + 4].try_into().ok()?,
        ))
    }

    fn get_book_id(&self, global_doc_id: u32) -> Option<String> {
        let local_id = global_doc_id.checked_sub(self.base_doc_id)?;
        if local_id >= self.num_docs {
            return None;
        }

        let num_chunks = self.num_docs as usize;
        let offsets_size = (num_chunks + 1) * 4;
        if offsets_size > self.chunks_mmap.len() {
            return None;
        }

        let start_pos = (local_id as usize) * 4;
        let start = u32::from_le_bytes(self.chunks_mmap[start_pos..start_pos + 4].try_into().ok()?)
            as usize;
        let end = u32::from_le_bytes(
            self.chunks_mmap[start_pos + 4..start_pos + 8]
                .try_into()
                .ok()?,
        ) as usize;

        let data_start = offsets_size + start;
        let data_end = offsets_size + end;
        if data_end > self.chunks_mmap.len() {
            return None;
        }

        String::from_utf8(self.chunks_mmap[data_start..data_end].to_vec()).ok()
    }
}

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

fn spawn_writer_thread(
    rx: crossbeam_channel::Receiver<BatchData>,
) -> thread::JoinHandle<Vec<(String, SegmentMeta)>> {
    thread::spawn(move || {
        let mut results = Vec::new();
        while let Ok(data) = rx.recv() {
            let segment_id = data.segment_id;
            let segment_name = format!("segment_{}", segment_id);

            match write_segment(data) {
                Ok(meta) => {
                    println!(
                        "  [Writer] Segment {} written: {} chunks",
                        segment_id, meta.num_docs
                    );
                    results.push((segment_name, meta));
                }
                Err(e) => {
                    eprintln!("  [Writer] Error writing segment {}: {}", segment_id, e);
                }
            }
        }
        results
    })
}

#[pyfunction]
#[pyo3(signature = (books_dir, index_dir, chunks_dir, stopwords, chunk_size=1000, chunk_overlap=100, batch_size=1000))]
pub fn index_corpus_file(
    py: Python<'_>,
    books_dir: String,
    index_dir: String,
    chunks_dir: String,
    stopwords: Vec<String>,
    chunk_size: usize,
    chunk_overlap: usize,
    batch_size: usize,
) -> PyResult<(u32, u32)> {
    let stopwords_set: FxHashSet<String> = stopwords.into_iter().collect();

    py.detach(|| {
        index_corpus_file_internal(
            &books_dir,
            &index_dir,
            &chunks_dir,
            &stopwords_set,
            chunk_size,
            chunk_overlap,
            batch_size,
        )
    })
}

fn index_corpus_file_internal(
    books_dir: &str,
    index_dir: &str,
    chunks_dir: &str,
    stopwords_set: &FxHashSet<String>,
    chunk_size: usize,
    chunk_overlap: usize,
    batch_size: usize,
) -> PyResult<(u32, u32)> {
    use glob::glob;
    use std::time::Instant;

    let start = Instant::now();
    let index_path = Path::new(index_dir);
    fs::create_dir_all(index_path).ok();
    fs::create_dir_all(chunks_dir).ok();

    println!("Scanning books directory...");
    let patterns = [
        format!("{}/*.epub", books_dir),
        format!("{}/*.txt", books_dir),
        format!("{}/*.pdf", books_dir),
    ];

    let mut book_files: Vec<String> = Vec::new();
    for pattern in &patterns {
        if let Ok(entries) = glob(pattern) {
            for entry in entries.flatten() {
                book_files.push(entry.to_string_lossy().to_string());
            }
        }
    }
    println!("Found {} files in {:?}", book_files.len(), start.elapsed());

    let (tx, rx) = bounded::<BatchData>(1);
    let writer_handle = spawn_writer_thread(rx);

    let total = book_files.len();
    let mut global_doc_id: u32 = 0;
    let mut indexed = 0u32;

    for (batch_idx, batch_start) in (0..total).step_by(batch_size).enumerate() {
        let batch_end = (batch_start + batch_size).min(total);
        let batch: Vec<_> = book_files[batch_start..batch_end].to_vec();

        println!(
            "Processing segment {} ({}-{} of {})...",
            batch_idx, batch_start, batch_end, total
        );
        let batch_start_time = Instant::now();

        let stopwords_clone = stopwords_set.clone();
        let chunks_dir_clone = chunks_dir.to_string();
        let base_doc_id = global_doc_id;

        let seen_hashes: DashMap<[u8; 32], ()> = DashMap::new();

        let docs: Vec<ProcessedDoc> = batch
            .par_iter()
            .filter_map(|path| {
                let book_id = Path::new(path).file_stem()?.to_string_lossy().to_string();
                let text = parse_file(path)?;

                let content_hash = blake3::hash(text.as_bytes());
                if seen_hashes.contains_key(content_hash.as_bytes()) {
                    return None;
                }
                seen_hashes.insert(*content_hash.as_bytes(), ());

                let chunks = chunk_text(&text, chunk_size, chunk_overlap);
                if chunks.is_empty() {
                    return None;
                }

                let shard = if book_id.len() < 2 {
                    format!("{:0>2}", book_id)
                } else {
                    book_id[..2].to_string()
                };
                let shard_dir = Path::new(&chunks_dir_clone).join(&shard);
                fs::create_dir_all(&shard_dir).ok();
                let chunk_path = shard_dir.join(format!("{}.zst", book_id));
                let full_text = chunks.join("\n");
                if let Ok(compressed) = zstd::stream::encode_all(full_text.as_bytes(), 3) {
                    fs::write(chunk_path, compressed).ok();
                }

                let mut chunk_data: Vec<(u32, FxHashMap<String, u32>)> =
                    Vec::with_capacity(chunks.len());
                // Arena for this document processing
                let mut bump = bumpalo::Bump::new();

                for chunk in &chunks {
                    bump.reset(); // Reuse memory for next chunk
                    let tokens = crate::analysis::analyze_arena(chunk, &bump);
                    if tokens.is_empty() {
                        continue;
                    }
                    let doc_length = tokens.len() as u32;
                    let mut freq_map: FxHashMap<&str, u32> = FxHashMap::default();
                    for token in tokens {
                        if !stopwords_clone.contains(token) {
                            *freq_map.entry(token).or_insert(0) += 1;
                        }
                    }
                    if !freq_map.is_empty() {
                        // Convert to owned strings only for the final map
                        let owned_map: FxHashMap<String, u32> = freq_map
                            .into_iter()
                            .map(|(k, v)| (k.to_string(), v))
                            .collect();
                        chunk_data.push((doc_length, owned_map));
                    }
                }

                if chunk_data.is_empty() {
                    return None;
                }

                Some(ProcessedDoc {
                    book_id,
                    chunks: chunk_data,
                })
            })
            .collect();

        let num_docs = docs.len();
        let num_chunks: usize = docs.iter().map(|d| d.chunks.len()).sum();
        indexed += num_docs as u32;
        global_doc_id += num_chunks as u32;

        let batch_data = BatchData {
            segment_id: batch_idx,
            segment_dir: index_path.join(format!("segment_{}", batch_idx)),
            docs,
            base_doc_id,
        };

        println!(
            "  [CPU] Segment {} processed in {:?}, {} books, {} chunks",
            batch_idx,
            batch_start_time.elapsed(),
            num_docs,
            num_chunks
        );

        tx.send(batch_data).unwrap();
    }

    drop(tx);
    println!("Waiting for writer to finish...");
    let segment_results = writer_handle.join().unwrap();

    let mut total_length: u64 = 0;
    let mut segment_names: Vec<String> = Vec::new();
    for (name, meta) in segment_results {
        total_length += meta.total_length;
        segment_names.push(name);
    }

    let avgdl = if global_doc_id > 0 {
        total_length as f32 / global_doc_id as f32
    } else {
        0.0
    };
    let index_meta = IndexMeta {
        segments: segment_names.clone(),
        total_docs: global_doc_id,
        avgdl,
    };
    let meta_json = serde_json::to_string_pretty(&index_meta)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    fs::write(index_path.join("index.json"), meta_json)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

    println!(
        "Done: {} books, {} chunks, {} segments in {:?}",
        indexed,
        global_doc_id,
        segment_names.len(),
        start.elapsed()
    );

    Ok((indexed, global_doc_id))
}
