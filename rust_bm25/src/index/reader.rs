use crate::index::segment::SegmentMeta;
use fst::automaton::Levenshtein;
use fst::{IntoStreamer, Map as FstMap, Streamer};
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
    #[inline(always)]
    fn read_u64(&self, mmap: &Mmap, pos: usize) -> Option<u64> {
        mmap.get(pos..pos + 8)
            .map(|slice| u64::from_le_bytes(slice.try_into().unwrap()))
    }

    #[inline(always)]
    fn read_u32(&self, mmap: &Mmap, pos: usize) -> Option<u32> {
        mmap.get(pos..pos + 4)
            .map(|slice| u32::from_le_bytes(slice.try_into().unwrap()))
    }

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

    pub fn get_doc_freq(&self, term: &str) -> Option<u32> {
        let term_idx = self.terms_fst.get(term)?;
        // 28 bytes per term
        let offset_pos = (term_idx as usize) * 28;

        // doc_count is at offset + 24 (4 bytes)
        self.read_u32(&self.offsets_mmap, offset_pos + 24)
    }

    pub fn get_postings_iter(&self, term: &str) -> Option<PostingsIter<'_>> {
        let term_idx = self.terms_fst.get(term)?;
        let offset_pos = (term_idx as usize) * 28;

        let doc_offset = self.read_u64(&self.offsets_mmap, offset_pos)?;
        let doc_len = self.read_u32(&self.offsets_mmap, offset_pos + 8)?;
        let freq_offset = self.read_u64(&self.offsets_mmap, offset_pos + 12)?;
        let freq_len = self.read_u32(&self.offsets_mmap, offset_pos + 20)?;
        let doc_count = self.read_u32(&self.offsets_mmap, offset_pos + 24)?;

        let doc_end = (doc_offset + doc_len as u64) as usize;
        let freq_end = (freq_offset + freq_len as u64) as usize;

        if doc_end > self.postings_docs_mmap.len() || freq_end > self.postings_freqs_mmap.len() {
            return None;
        }

        Some(PostingsIter::new(
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
        self.read_u32(&self.doc_lengths_mmap, pos)
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
        let start = self.read_u32(&self.chunks_mmap, start_pos)? as usize;
        let end = self.read_u32(&self.chunks_mmap, start_pos + 4)? as usize;

        let data_start = offsets_size + start;
        let data_end = offsets_size + end;
        if data_end > self.chunks_mmap.len() {
            return None;
        }

        std::str::from_utf8(self.chunks_mmap.get(data_start..data_end)?)
            .map(|s| s.to_string())
            .ok()
    }

    pub fn get_fuzzy_terms(&self, term: &str, max_dist: u32) -> Vec<String> {
        let lev = match Levenshtein::new(term, max_dist) {
            Ok(l) => l,
            Err(_) => return vec![],
        };

        let mut stream = self.terms_fst.search(lev).into_stream();
        let mut results = Vec::new();

        while let Some((key, _)) = stream.next() {
            if let Ok(s) = std::str::from_utf8(key) {
                results.push(s.to_string());
            }
        }
        results
    }
}

use bitpacking::{BitPacker, BitPacker4x};
const BLOCK_LEN: usize = 128;

pub struct PostingsIter<'a> {
    doc_data: &'a [u8],
    freq_data: &'a [u8],
    doc_pos: usize,
    freq_pos: usize,

    current_doc: u32,
    count_left: usize,

    // SIMD buffers
    doc_buffer: [u32; BLOCK_LEN],
    freq_buffer: [u32; BLOCK_LEN],
    buffer_idx: usize,
    buffer_len: usize,

    bitpacker: BitPacker4x,
}

impl<'a> PostingsIter<'a> {
    pub fn new(doc_data: &'a [u8], freq_data: &'a [u8], doc_count: usize) -> Self {
        Self {
            doc_data,
            freq_data,
            doc_pos: 0,
            freq_pos: 0,
            current_doc: 0,
            count_left: doc_count,
            doc_buffer: [0u32; BLOCK_LEN],
            freq_buffer: [0u32; BLOCK_LEN],
            buffer_idx: BLOCK_LEN, // Force refill on first next()
            buffer_len: 0,
            bitpacker: BitPacker4x::new(),
        }
    }

    // Helper for varint decoding (copied from codecs to avoid pub dep)
    fn decode_varint(&self, data: &[u8], mut pos: usize) -> (u32, usize) {
        let mut result = 0u32;
        let mut shift = 0;
        loop {
            if pos >= data.len() {
                return (result, pos);
            }
            let byte = unsafe { *data.get_unchecked(pos) };
            pos += 1;
            result |= ((byte & 0x7F) as u32) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        (result, pos)
    }

    fn refill_buffer(&mut self) {
        // If we have at least BLOCK_LEN items left, we used bitpacking
        if self.count_left >= BLOCK_LEN {
            // Docs
            let doc_bits = self.doc_data[self.doc_pos];
            self.doc_pos += 1;
            let doc_bytes = (doc_bits as usize) * 16;
            self.bitpacker.decompress(
                &self.doc_data[self.doc_pos..self.doc_pos + doc_bytes],
                &mut self.doc_buffer,
                doc_bits,
            );
            self.doc_pos += doc_bytes;

            // Freqs
            let freq_bits = self.freq_data[self.freq_pos];
            self.freq_pos += 1;
            let freq_bytes = (freq_bits as usize) * 16;
            self.bitpacker.decompress(
                &self.freq_data[self.freq_pos..self.freq_pos + freq_bytes],
                &mut self.freq_buffer,
                freq_bits,
            );
            self.freq_pos += freq_bytes;

            self.buffer_len = BLOCK_LEN;
        } else {
            // Remaining items are VarInt encoded
            // We'll decode them one by one in next() or fill buffer here?
            // Let's fill buffer to keep next() simple
            for i in 0..self.count_left {
                let (delta, new_pos) = self.decode_varint(self.doc_data, self.doc_pos);
                self.doc_pos = new_pos;
                self.doc_buffer[i] = delta;

                let (tf, new_pos) = self.decode_varint(self.freq_data, self.freq_pos);
                self.freq_pos = new_pos;
                self.freq_buffer[i] = tf;
            }
            self.buffer_len = self.count_left;
        }

        self.buffer_idx = 0;
    }
}

impl<'a> Iterator for PostingsIter<'a> {
    type Item = (u32, u32);

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        if self.count_left == 0 {
            return None;
        }

        if self.buffer_idx >= self.buffer_len {
            self.refill_buffer();
        }

        let delta = self.doc_buffer[self.buffer_idx];
        let freq = self.freq_buffer[self.buffer_idx];

        self.current_doc += delta;
        self.buffer_idx += 1;
        self.count_left -= 1;

        Some((self.current_doc, freq))
    }
}
