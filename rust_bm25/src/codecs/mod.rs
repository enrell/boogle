use pyo3::prelude::*;
use pyo3::types::PyBytes;

use bitpacking::{BitPacker, BitPacker4x};

const BLOCK_LEN: usize = 128;

pub fn encode_postings_separated(postings: &[(u32, u32)]) -> (Vec<u8>, Vec<u8>) {
    let mut sorted: Vec<_> = postings.to_vec();
    sorted.sort_unstable_by_key(|p| p.0);

    let len = sorted.len();
    let mut docs_buf = Vec::with_capacity(len * 4);
    let mut freqs_buf = Vec::with_capacity(len * 4);

    let bitpacker = BitPacker4x::new();
    let mut chunk_docs = [0u32; BLOCK_LEN];
    let mut chunk_freqs = [0u32; BLOCK_LEN];

    // We need 128 * 4 = 512 bytes output buffer max per block
    let mut compressed_buf = [0u8; BLOCK_LEN * 4];

    let mut prev_doc_id = 0u32;
    let mut count = 0;

    for (doc_id, tf) in sorted {
        let delta = doc_id - prev_doc_id;
        prev_doc_id = doc_id;

        chunk_docs[count] = delta;
        chunk_freqs[count] = tf;
        count += 1;

        if count == BLOCK_LEN {
            // Compress Docs
            let num_bits = bitpacker.num_bits(&chunk_docs);
            docs_buf.push(num_bits);
            let compressed_len = bitpacker.compress(&chunk_docs, &mut compressed_buf[..], num_bits);
            docs_buf.extend_from_slice(&compressed_buf[..compressed_len]);

            // Compress Freqs
            let num_bits = bitpacker.num_bits(&chunk_freqs);
            freqs_buf.push(num_bits);
            let compressed_len =
                bitpacker.compress(&chunk_freqs, &mut compressed_buf[..], num_bits);
            freqs_buf.extend_from_slice(&compressed_buf[..compressed_len]);

            count = 0;
        }
    }

    // Remaining items - encode with VarInt
    for i in 0..count {
        encode_varint(chunk_docs[i], &mut docs_buf);
        encode_varint(chunk_freqs[i], &mut freqs_buf);
    }

    (docs_buf, freqs_buf)
}

#[allow(dead_code)]
pub fn decode_postings_separated(
    doc_data: &[u8],
    tf_data: &[u8],
    num_postings: usize,
) -> Vec<(u32, u32)> {
    let mut result: Vec<(u32, u32)> = Vec::with_capacity(num_postings);
    let mut doc_pos = 0;
    let mut tf_pos = 0;

    let bitpacker = BitPacker4x::new();
    let mut decompressed_docs = [0u32; BLOCK_LEN];
    let mut decompressed_freqs = [0u32; BLOCK_LEN];

    let mut doc_ids_processed = 0;
    let mut doc_id_accum = 0u32;

    while doc_ids_processed + BLOCK_LEN <= num_postings {
        // Doc Block
        let doc_bits = doc_data[doc_pos];
        doc_pos += 1;
        let doc_bytes = (doc_bits as usize) * 16;
        bitpacker.decompress(
            &doc_data[doc_pos..doc_pos + doc_bytes],
            &mut decompressed_docs,
            doc_bits,
        );
        doc_pos += doc_bytes;

        // Freq Block
        let freq_bits = tf_data[tf_pos];
        tf_pos += 1;
        let freq_bytes = (freq_bits as usize) * 16;
        bitpacker.decompress(
            &tf_data[tf_pos..tf_pos + freq_bytes],
            &mut decompressed_freqs,
            freq_bits,
        );
        tf_pos += freq_bytes;

        for i in 0..BLOCK_LEN {
            doc_id_accum += decompressed_docs[i];
            result.push((doc_id_accum, decompressed_freqs[i]));
        }
        doc_ids_processed += BLOCK_LEN;
    }

    // Tail (VarInt)
    for _ in doc_ids_processed..num_postings {
        let (delta, new_pos) = decode_varint(doc_data, doc_pos);
        doc_pos = new_pos;

        let (tf, new_tf_pos) = decode_varint(tf_data, tf_pos);
        tf_pos = new_tf_pos;

        doc_id_accum += delta;
        result.push((doc_id_accum, tf));
    }

    result
}

// Legacy internal encoding (interleaved)
pub fn encode_postings_internal(postings: &[(u32, u32)]) -> Vec<u8> {
    let mut sorted: Vec<_> = postings.to_vec();
    sorted.sort_unstable_by_key(|p| p.0);

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

pub fn decode_postings_internal(data: &[u8]) -> Vec<(u32, u32)> {
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
pub fn encode_postings(py: Python<'_>, postings: Vec<(u32, u32)>) -> Py<PyBytes> {
    let result = encode_postings_internal(&postings);
    PyBytes::new(py, &result).into()
}

#[pyfunction]
pub fn decode_postings(data: &[u8]) -> Vec<(u32, u32)> {
    decode_postings_internal(data)
}

#[pyfunction]
pub fn merge_postings(py: Python<'_>, a: &[u8], b: &[u8]) -> Py<PyBytes> {
    let mut postings_a = decode_postings_internal(a);
    let postings_b = decode_postings_internal(b);
    postings_a.extend(postings_b);
    let result = encode_postings_internal(&postings_a);
    PyBytes::new(py, &result).into()
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
