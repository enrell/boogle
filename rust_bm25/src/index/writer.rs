use crate::analysis::analyze_arena;
use crate::codecs::encode_postings_separated;
use crate::document::parsers::{chunk_text, parse_file};
use crate::index::segment::{BatchData, IndexMeta, ProcessedDoc, SegmentMeta};
use crossbeam_channel::bounded;
use dashmap::DashMap;
use fst::Map as FstMap;
use pyo3::prelude::*;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use std::fs;
use std::path::Path;
use std::thread;

const OFFSET_SIZE: usize = 28;

pub(crate) fn write_segment(data: BatchData) -> std::io::Result<SegmentMeta> {
    fs::create_dir_all(&data.segment_dir)?;

    let (book_ids, chunk_to_book, doc_lengths, chunk_freq_maps, total_length) =
        collect_chunks(&data);

    let inverted_index = build_inverted_index(chunk_freq_maps);

    let mut sorted_terms: Vec<_> = inverted_index
        .into_iter()
        .filter(|(_, postings)| !postings.is_empty())
        .collect();
    sorted_terms.par_sort_unstable_by(|a, b| a.0.cmp(&b.0));

    let encoded_postings: Vec<_> = sorted_terms
        .par_iter()
        .map(|(_, postings)| encode_postings_separated(postings))
        .collect();

    let (term_offsets, offsets_data) = build_offsets(&sorted_terms, &encoded_postings);

    let (postings_docs_blob, postings_freqs_blob) = merge_postings(&encoded_postings);

    let fst_bytes = build_fst(term_offsets)?;
    let chunks_blob = build_chunks_blob(&book_ids, &chunk_to_book);
    let lengths_blob = build_lengths_blob(&doc_lengths);

    write_segment_files(
        &data.segment_dir,
        &postings_docs_blob,
        &postings_freqs_blob,
        &fst_bytes,
        &offsets_data,
        &chunks_blob,
        &lengths_blob,
    )?;

    let meta = SegmentMeta {
        num_docs: chunk_to_book.len() as u32,
        base_doc_id: data.base_doc_id,
        total_length,
    };

    fs::write(
        data.segment_dir.join("meta.json"),
        serde_json::to_string(&meta)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?,
    )?;

    Ok(meta)
}

fn collect_chunks(
    data: &BatchData,
) -> (
    Vec<String>,
    Vec<u16>,
    Vec<u32>,
    Vec<(u32, FxHashMap<String, u32>)>,
    u64,
) {
    let total_chunks: usize = data.docs.iter().map(|d| d.chunks.len()).sum();
    let mut book_ids = Vec::with_capacity(data.docs.len());
    let mut chunk_to_book = Vec::with_capacity(total_chunks);
    let mut doc_lengths = Vec::with_capacity(total_chunks);
    let mut chunk_freq_maps = Vec::with_capacity(total_chunks);
    let mut total_length = 0u64;

    for doc in &data.docs {
        let book_idx = book_ids.len() as u16;
        book_ids.push(doc.book_id.clone());

        for (doc_length, freq_map) in &doc.chunks {
            let doc_id = data.base_doc_id + chunk_to_book.len() as u32;
            chunk_to_book.push(book_idx);
            doc_lengths.push(*doc_length);
            total_length += *doc_length as u64;
            chunk_freq_maps.push((doc_id, freq_map.clone()));
        }
    }

    (
        book_ids,
        chunk_to_book,
        doc_lengths,
        chunk_freq_maps,
        total_length,
    )
}

fn build_inverted_index(
    chunk_freq_maps: Vec<(u32, FxHashMap<String, u32>)>,
) -> FxHashMap<String, Vec<(u32, u32)>> {
    let mut terms: FxHashMap<String, Vec<(u32, u32)>> = FxHashMap::default();
    for (doc_id, freq_map) in chunk_freq_maps {
        for (term, freq) in freq_map {
            terms.entry(term).or_default().push((doc_id, freq));
        }
    }
    terms
}

fn build_offsets(
    sorted_terms: &[(String, Vec<(u32, u32)>)],
    encoded_postings: &[(Vec<u8>, Vec<u8>)],
) -> (Vec<(&str, u64)>, Vec<u8>) {
    let mut term_offsets = Vec::with_capacity(sorted_terms.len());
    let mut offsets_data = Vec::with_capacity(sorted_terms.len() * OFFSET_SIZE);
    let mut current_offset_doc = 0u64;
    let mut current_offset_freq = 0u64;

    for (idx, (term, original_postings)) in sorted_terms.iter().enumerate() {
        let (docs_data, freqs_data) = &encoded_postings[idx];
        let len_doc = docs_data.len() as u32;
        let len_freq = freqs_data.len() as u32;
        let doc_count = original_postings.len() as u32;

        offsets_data.extend_from_slice(&current_offset_doc.to_le_bytes());
        offsets_data.extend_from_slice(&len_doc.to_le_bytes());
        offsets_data.extend_from_slice(&current_offset_freq.to_le_bytes());
        offsets_data.extend_from_slice(&len_freq.to_le_bytes());
        offsets_data.extend_from_slice(&doc_count.to_le_bytes());

        term_offsets.push((term.as_str(), idx as u64));

        current_offset_doc += len_doc as u64;
        current_offset_freq += len_freq as u64;
    }

    (term_offsets, offsets_data)
}

fn merge_postings(encoded_postings: &[(Vec<u8>, Vec<u8>)]) -> (Vec<u8>, Vec<u8>) {
    let total_docs: usize = encoded_postings.iter().map(|(d, _)| d.len()).sum();
    let total_freqs: usize = encoded_postings.iter().map(|(_, f)| f.len()).sum();

    let mut docs_blob = Vec::with_capacity(total_docs);
    let mut freqs_blob = Vec::with_capacity(total_freqs);

    for (d, f) in encoded_postings {
        docs_blob.extend_from_slice(d);
        freqs_blob.extend_from_slice(f);
    }

    (docs_blob, freqs_blob)
}

fn build_fst(term_offsets: Vec<(&str, u64)>) -> std::io::Result<Vec<u8>> {
    let fst_map = FstMap::from_iter(term_offsets)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    Ok(fst_map.as_fst().as_bytes().to_vec())
}

fn build_chunks_blob(book_ids: &[String], chunk_to_book: &[u16]) -> Vec<u8> {
    let mut chunks_data: Vec<u8> = Vec::with_capacity(book_ids.iter().map(|s| s.len()).sum());
    let mut offsets: Vec<u32> = Vec::with_capacity(chunk_to_book.len() + 1);

    for &book_idx in chunk_to_book {
        offsets.push(chunks_data.len() as u32);
        chunks_data.extend_from_slice(book_ids[book_idx as usize].as_bytes());
    }
    offsets.push(chunks_data.len() as u32);

    let mut blob = Vec::with_capacity((offsets.len() * 4) + chunks_data.len());
    for offset in offsets {
        blob.extend_from_slice(&offset.to_le_bytes());
    }
    blob.extend_from_slice(&chunks_data);
    blob
}

fn build_lengths_blob(doc_lengths: &[u32]) -> Vec<u8> {
    let mut blob = Vec::with_capacity(doc_lengths.len() * 4);
    for len in doc_lengths {
        blob.extend_from_slice(&len.to_le_bytes());
    }
    blob
}

fn write_segment_files(
    segment_dir: &Path,
    docs_blob: &[u8],
    freqs_blob: &[u8],
    fst_bytes: &[u8],
    offsets_data: &[u8],
    chunks_blob: &[u8],
    lengths_blob: &[u8],
) -> std::io::Result<()> {
    fs::write(segment_dir.join("postings_docs.bin"), docs_blob)?;
    fs::write(segment_dir.join("postings_freqs.bin"), freqs_blob)?;
    fs::write(segment_dir.join("terms.fst"), fst_bytes)?;
    fs::write(segment_dir.join("offsets.bin"), offsets_data)?;
    fs::write(segment_dir.join("chunks.bin"), chunks_blob)?;
    fs::write(segment_dir.join("doc_lengths.bin"), lengths_blob)?;
    Ok(())
}

fn spawn_writer_thread(
    rx: crossbeam_channel::Receiver<BatchData>,
) -> thread::JoinHandle<Vec<(String, SegmentMeta)>> {
    thread::spawn(move || {
        let mut results = Vec::new();
        while let Ok(data) = rx.recv() {
            let segment_name = format!("segment_{}", data.segment_id);
            match write_segment(data) {
                Ok(meta) => {
                    println!("  [Writer] Segment written: {} chunks", meta.num_docs);
                    results.push((segment_name, meta));
                }
                Err(e) => eprintln!("  [Writer] Error: {}", e),
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
        index_corpus_internal(
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

fn index_corpus_internal(
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

    let book_files = collect_book_files(books_dir);
    println!("Found {} files in {:?}", book_files.len(), start.elapsed());

    let (tx, rx) = bounded::<BatchData>(1);
    let writer_handle = spawn_writer_thread(rx);

    let seen_hashes: DashMap<[u8; 32], ()> = DashMap::new();
    let mut global_doc_id = 0u32;
    let mut indexed = 0u32;
    let total = book_files.len();

    for (batch_idx, batch_start) in (0..total).step_by(batch_size).enumerate() {
        let batch_end = (batch_start + batch_size).min(total);
        let batch = &book_files[batch_start..batch_end];

        let docs = process_batch(
            batch,
            chunks_dir,
            stopwords_set,
            chunk_size,
            chunk_overlap,
            &seen_hashes,
        );

        let num_chunks: usize = docs.iter().map(|d| d.chunks.len()).sum();
        indexed += docs.len() as u32;

        let batch_data = BatchData {
            segment_id: batch_idx,
            segment_dir: index_path.join(format!("segment_{}", batch_idx)),
            docs,
            base_doc_id: global_doc_id,
        };

        global_doc_id += num_chunks as u32;
        tx.send(batch_data).unwrap();
    }

    drop(tx);
    let segment_results = writer_handle.join().unwrap();

    write_index_meta(index_path, &segment_results, global_doc_id)?;

    println!(
        "Done: {} books, {} chunks, {} segments in {:?}",
        indexed,
        global_doc_id,
        segment_results.len(),
        start.elapsed()
    );

    Ok((indexed, global_doc_id))
}

fn collect_book_files(books_dir: &str) -> Vec<String> {
    use glob::glob;

    let patterns = [
        format!("{}/*.epub", books_dir),
        format!("{}/*.txt", books_dir),
        format!("{}/*.pdf", books_dir),
    ];

    let mut files = Vec::new();
    for pattern in &patterns {
        if let Ok(entries) = glob(pattern) {
            for entry in entries.flatten() {
                files.push(entry.to_string_lossy().to_string());
            }
        }
    }
    files
}

fn process_batch(
    paths: &[String],
    chunks_dir: &str,
    stopwords: &FxHashSet<String>,
    chunk_size: usize,
    chunk_overlap: usize,
    seen_hashes: &DashMap<[u8; 32], ()>,
) -> Vec<ProcessedDoc> {
    paths
        .par_iter()
        .filter_map(|path| {
            process_single_book(
                path,
                chunks_dir,
                stopwords,
                chunk_size,
                chunk_overlap,
                seen_hashes,
            )
        })
        .collect()
}

fn process_single_book(
    path: &str,
    chunks_dir: &str,
    stopwords: &FxHashSet<String>,
    chunk_size: usize,
    chunk_overlap: usize,
    seen_hashes: &DashMap<[u8; 32], ()>,
) -> Option<ProcessedDoc> {
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

    save_chunks_to_disk(&book_id, &chunks, chunks_dir);
    let chunk_data = analyze_chunks(&chunks, stopwords);

    if chunk_data.is_empty() {
        return None;
    }

    Some(ProcessedDoc {
        book_id,
        chunks: chunk_data,
    })
}

fn save_chunks_to_disk(book_id: &str, chunks: &[String], chunks_dir: &str) {
    let shard = if book_id.len() < 2 {
        format!("{:0>2}", book_id)
    } else {
        book_id[..2].to_string()
    };

    let shard_dir = Path::new(chunks_dir).join(&shard);
    fs::create_dir_all(&shard_dir).ok();

    let chunk_path = shard_dir.join(format!("{}.zst", book_id));
    let full_text = chunks.join("\n");

    if let Ok(compressed) = zstd::stream::encode_all(full_text.as_bytes(), 3) {
        fs::write(chunk_path, compressed).ok();
    }
}

fn analyze_chunks(
    chunks: &[String],
    stopwords: &FxHashSet<String>,
) -> Vec<(u32, FxHashMap<String, u32>)> {
    let mut bump = bumpalo::Bump::new();
    let mut result = Vec::with_capacity(chunks.len());

    for chunk in chunks {
        bump.reset();
        let tokens = analyze_arena(chunk, &bump);
        if tokens.is_empty() {
            continue;
        }

        let doc_length = tokens.len() as u32;
        let mut freq_map: FxHashMap<&str, u32> = FxHashMap::default();

        for token in tokens {
            if !stopwords.contains(token) {
                *freq_map.entry(token).or_insert(0) += 1;
            }
        }

        if !freq_map.is_empty() {
            let owned_map: FxHashMap<String, u32> = freq_map
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect();
            result.push((doc_length, owned_map));
        }
    }

    result
}

fn write_index_meta(
    index_path: &Path,
    segment_results: &[(String, SegmentMeta)],
    total_docs: u32,
) -> PyResult<()> {
    let total_length: u64 = segment_results.iter().map(|(_, m)| m.total_length).sum();
    let segment_names: Vec<_> = segment_results.iter().map(|(n, _)| n.clone()).collect();

    let avgdl = if total_docs > 0 {
        total_length as f32 / total_docs as f32
    } else {
        0.0
    };

    let index_meta = IndexMeta {
        segments: segment_names,
        total_docs,
        avgdl,
    };

    let meta_json = serde_json::to_string_pretty(&index_meta)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

    fs::write(index_path.join("index.json"), meta_json)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

    Ok(())
}
