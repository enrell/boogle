use bitpacking::{BitPacker, BitPacker4x};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

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
    let mut compressed_buf = [0u8; BLOCK_LEN * 4];

    let mut prev_doc_id = 0u32;
    let mut count = 0;

    for (doc_id, tf) in sorted {
        chunk_docs[count] = doc_id - prev_doc_id;
        chunk_freqs[count] = tf;
        prev_doc_id = doc_id;
        count += 1;

        if count == BLOCK_LEN {
            compress_block(&bitpacker, &chunk_docs, &mut docs_buf, &mut compressed_buf);
            compress_block(
                &bitpacker,
                &chunk_freqs,
                &mut freqs_buf,
                &mut compressed_buf,
            );
            count = 0;
        }
    }

    for i in 0..count {
        encode_varint(chunk_docs[i], &mut docs_buf);
        encode_varint(chunk_freqs[i], &mut freqs_buf);
    }

    (docs_buf, freqs_buf)
}

fn compress_block(
    packer: &BitPacker4x,
    data: &[u32; BLOCK_LEN],
    output: &mut Vec<u8>,
    buf: &mut [u8; BLOCK_LEN * 4],
) {
    let num_bits = packer.num_bits(data);
    output.push(num_bits);
    let len = packer.compress(data, &mut buf[..], num_bits);
    output.extend_from_slice(&buf[..len]);
}

#[allow(dead_code)]
pub fn decode_postings_separated(
    doc_data: &[u8],
    tf_data: &[u8],
    num_postings: usize,
) -> Vec<(u32, u32)> {
    let mut result = Vec::with_capacity(num_postings);
    let bitpacker = BitPacker4x::new();
    let mut decompressed_docs = [0u32; BLOCK_LEN];
    let mut decompressed_freqs = [0u32; BLOCK_LEN];

    let mut doc_pos = 0;
    let mut tf_pos = 0;
    let mut doc_id_accum = 0u32;
    let mut processed = 0;

    while processed + BLOCK_LEN <= num_postings {
        doc_pos = decompress_block(&bitpacker, doc_data, doc_pos, &mut decompressed_docs);
        tf_pos = decompress_block(&bitpacker, tf_data, tf_pos, &mut decompressed_freqs);

        for i in 0..BLOCK_LEN {
            doc_id_accum += decompressed_docs[i];
            result.push((doc_id_accum, decompressed_freqs[i]));
        }
        processed += BLOCK_LEN;
    }

    for _ in processed..num_postings {
        let (delta, new_pos) = decode_varint(doc_data, doc_pos);
        doc_pos = new_pos;
        let (tf, new_tf_pos) = decode_varint(tf_data, tf_pos);
        tf_pos = new_tf_pos;
        doc_id_accum += delta;
        result.push((doc_id_accum, tf));
    }

    result
}

fn decompress_block(
    packer: &BitPacker4x,
    data: &[u8],
    mut pos: usize,
    output: &mut [u32; BLOCK_LEN],
) -> usize {
    let bits = data[pos];
    pos += 1;
    let bytes = (bits as usize) * 16;
    packer.decompress(&data[pos..pos + bytes], output, bits);
    pos + bytes
}

pub fn encode_postings_internal(postings: &[(u32, u32)]) -> Vec<u8> {
    let mut sorted: Vec<_> = postings.to_vec();
    sorted.sort_unstable_by_key(|p| p.0);

    let mut result = Vec::with_capacity(sorted.len() * 4);
    let mut prev_doc_id = 0u32;

    for (doc_id, tf) in sorted {
        encode_varint(doc_id - prev_doc_id, &mut result);
        encode_varint(tf, &mut result);
        prev_doc_id = doc_id;
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
    PyBytes::new(py, &encode_postings_internal(&postings)).into()
}

#[pyfunction]
pub fn decode_postings(data: &[u8]) -> Vec<(u32, u32)> {
    decode_postings_internal(data)
}

#[pyfunction]
pub fn merge_postings(py: Python<'_>, a: &[u8], b: &[u8]) -> Py<PyBytes> {
    let mut postings = decode_postings_internal(a);
    postings.extend(decode_postings_internal(b));
    PyBytes::new(py, &encode_postings_internal(&postings)).into()
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
