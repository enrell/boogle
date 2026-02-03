use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SegmentMeta {
    pub num_docs: u32,
    pub base_doc_id: u32,
    pub total_length: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct IndexMeta {
    pub segments: Vec<String>,
    pub total_docs: u32,
    pub avgdl: f32,
}

pub struct ProcessedDoc {
    pub book_id: String,
    pub chunks: Vec<(u32, FxHashMap<String, u32>)>,
}

pub struct BatchData {
    pub segment_id: usize,
    pub segment_dir: PathBuf,
    pub docs: Vec<ProcessedDoc>,
    pub base_doc_id: u32,
}
