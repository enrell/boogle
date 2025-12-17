use crate::codecs::decode_postings_separated;
use crate::index::segment::SegmentMeta;
use fst::Map as FstMap;
use memmap2::Mmap;
use std::fs::{self, File};
use std::path::Path;

pub struct SegmentReader {
    pub terms_fst: FstMap<Mmap>,
    pub offsets_mmap: Mmap,
    pub postings_docs_mmap: Mmap,
    pub postings_freqs_mmap: Mmap,
    pub chunks_mmap: Mmap,
    pub doc_lengths_mmap: Mmap,
    pub base_doc_id: u32,
    pub num_docs: u32,
}

impl SegmentReader {
    pub fn open(segment_dir: &Path) -> std::io::Result<Self> {
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

    pub fn get_postings(&self, term: &str) -> Option<Vec<(u32, u32)>> {
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

        Some(decode_postings_separated(
            &self.postings_docs_mmap[doc_offset as usize..doc_end],
            &self.postings_freqs_mmap[freq_offset as usize..freq_end],
            doc_count as usize,
        ))
    }

    pub fn get_doc_length(&self, global_doc_id: u32) -> Option<u32> {
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

    pub fn get_book_id(&self, global_doc_id: u32) -> Option<String> {
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
