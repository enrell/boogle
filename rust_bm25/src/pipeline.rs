use crate::document::parsers::{chunk_text, parse_bytes};
use crate::index::segment::{BatchData, ProcessedDoc};
use crate::index::writer::write_segment;
use flume::bounded as flume_bounded;
use pyo3::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use tokio::runtime::Runtime;

struct RawBook {
    id: String,
    content: Vec<u8>,
    extension: String,
}

struct PipelineConfig {
    index_dir: PathBuf,
    chunks_dir: PathBuf,
    stopwords: Arc<FxHashSet<String>>,
}

#[pyfunction]
pub fn run_streaming_pipeline(
    py: Python<'_>,
    items: Vec<(String, String, String)>,
    index_dir: String,
    chunks_dir: String,
    stopwords: Vec<String>,
) -> PyResult<()> {
    py.detach(|| run_pipeline_internal(items, index_dir, chunks_dir, stopwords))
}

fn run_pipeline_internal(
    items: Vec<(String, String, String)>,
    index_dir: String,
    chunks_dir: String,
    stopwords: Vec<String>,
) -> PyResult<()> {
    let rt = Runtime::new().map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

    let chunks_dir_path = PathBuf::from(&chunks_dir);
    let index_dir_path = PathBuf::from(&index_dir);
    fs::create_dir_all(&chunks_dir_path).ok();
    fs::create_dir_all(&index_dir_path).ok();

    let config = Arc::new(PipelineConfig {
        index_dir: index_dir_path.clone(),
        chunks_dir: chunks_dir_path.clone(),
        stopwords: Arc::new(stopwords.into_iter().collect()),
    });

    let (tx_raw, rx_raw) = flume_bounded::<RawBook>(50);
    let (tx_processed, rx_processed) = flume_bounded::<ProcessedDoc>(500);

    let indexer_handle = spawn_indexer_stage(rx_processed, config.clone());

    let processor_handles = spawn_processor_stage(rx_raw, tx_processed, config.clone());

    rt.block_on(async {
        run_downloader_stage(items, tx_raw, config.chunks_dir.clone()).await;
    });

    drop(processor_handles);

    let _ = indexer_handle.join();

    Ok(())
}

fn spawn_indexer_stage(
    rx: flume::Receiver<ProcessedDoc>,
    config: Arc<PipelineConfig>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut batch = Vec::new();
        let mut global_doc_id = 0u32;
        let mut segment_id = 0;

        while let Ok(doc) = rx.recv() {
            batch.push(doc);

            if batch.len() >= 1000 {
                write_batch(&mut batch, &mut global_doc_id, &mut segment_id, &config);
            }
        }

        if !batch.is_empty() {
            write_batch(&mut batch, &mut global_doc_id, &mut segment_id, &config);
        }
    })
}

fn write_batch(
    batch: &mut Vec<ProcessedDoc>,
    global_doc_id: &mut u32,
    segment_id: &mut usize,
    config: &PipelineConfig,
) {
    println!(
        "Indexing batch of {} books (Seg {})...",
        batch.len(),
        segment_id
    );

    let batch_data = BatchData {
        segment_id: *segment_id,
        segment_dir: config.index_dir.join(format!("segment_{}", segment_id)),
        docs: std::mem::take(batch),
        base_doc_id: *global_doc_id,
    };

    match write_segment(batch_data) {
        Ok(meta) => {
            *global_doc_id += meta.num_docs;
        }
        Err(e) => {
            eprintln!("Failed to write segment {}: {}", segment_id, e);
        }
    }

    *segment_id += 1;
}

fn spawn_processor_stage(
    rx: flume::Receiver<RawBook>,
    tx: flume::Sender<ProcessedDoc>,
    config: Arc<PipelineConfig>,
) -> Vec<thread::JoinHandle<()>> {
    let num_workers = num_cpus::get();
    let mut handles = Vec::with_capacity(num_workers);

    for _ in 0..num_workers {
        let rx = rx.clone();
        let tx = tx.clone();
        let config = config.clone();

        handles.push(thread::spawn(move || {
            while let Ok(raw) = rx.recv() {
                process_book(raw, &tx, &config);
            }
        }));
    }
    handles
}

fn process_book(raw: RawBook, tx: &flume::Sender<ProcessedDoc>, config: &PipelineConfig) {
    if !should_process(&raw.id, config) {
        return;
    }

    let text = match parse_bytes(&raw.content, &raw.extension) {
        Some(t) => t,
        None => return,
    };

    let chunks = chunk_text(&text, 1000, 100);
    if chunks.is_empty() {
        return;
    }

    save_content(&raw.id, &chunks, config);

    if let Some(doc) = analyze_content(raw.id, chunks, config) {
        let _ = tx.send(doc);
    }
}

fn should_process(id: &str, config: &PipelineConfig) -> bool {
    let shard = if id.len() < 2 {
        format!("{:0>2}", id)
    } else {
        id[..2].to_string()
    };
    let shard_dir = config.chunks_dir.join(&shard);
    let chunk_path = shard_dir.join(format!("{}.zst", id));
    !chunk_path.exists()
}

fn save_content(id: &str, chunks: &[String], config: &PipelineConfig) {
    let shard = if id.len() < 2 {
        format!("{:0>2}", id)
    } else {
        id[..2].to_string()
    };
    let shard_dir = config.chunks_dir.join(&shard);
    let chunk_path = shard_dir.join(format!("{}.zst", id));

    let full_text = chunks.join("\n");
    let _ = fs::create_dir_all(&shard_dir);
    if let Ok(compressed) = zstd::stream::encode_all(full_text.as_bytes(), 3) {
        let _ = fs::write(&chunk_path, compressed);
    }
}

fn analyze_content(
    id: String,
    chunks: Vec<String>,
    config: &PipelineConfig,
) -> Option<ProcessedDoc> {
    let mut chunk_data = Vec::with_capacity(chunks.len());
    let mut bump = bumpalo::Bump::new();

    for chunk in &chunks {
        bump.reset();
        let tokens = crate::analysis::analyze_arena(chunk, &bump);
        if tokens.is_empty() {
            continue;
        }

        let token_len = tokens.len() as u32;
        let mut freq_map: FxHashMap<String, u32> = FxHashMap::default();

        for token in tokens {
            if !config.stopwords.contains(token) {
                *freq_map.entry(token.to_string()).or_insert(0) += 1;
            }
        }

        if !freq_map.is_empty() {
            chunk_data.push((token_len, freq_map));
        }
    }

    if chunk_data.is_empty() {
        None
    } else {
        Some(ProcessedDoc {
            book_id: id,
            chunks: chunk_data,
        })
    }
}

async fn run_downloader_stage(
    items: Vec<(String, String, String)>,
    tx: flume::Sender<RawBook>,
    chunks_dir: PathBuf,
) {
    let semaphore = Arc::new(tokio::sync::Semaphore::new(20));
    let client = reqwest::Client::new();
    let mut handles = Vec::new();

    for (id, url, extension) in items {
        let sem = semaphore.clone();
        let tx = tx.clone();
        let client = client.clone();
        let chunks_dir = chunks_dir.clone();

        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();

            let shard = if id.len() < 2 {
                format!("{:0>2}", id)
            } else {
                id[..2].to_string()
            };
            if chunks_dir.join(&shard).join(format!("{}.zst", id)).exists() {
                return;
            }

            match client.get(&url).send().await {
                Ok(resp) => {
                    if let Ok(bytes) = resp.bytes().await {
                        let _ = tx
                            .send_async(RawBook {
                                id,
                                content: bytes.to_vec(),
                                extension,
                            })
                            .await;
                    }
                }
                Err(e) => eprintln!("Failed to download {}: {}", url, e),
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }
}
