use pyo3::prelude::*;
use pyo3::types::PyBytes;

pub fn encode_postings_internal(postings: &[(u32, u32)]) -> Vec<u8> {
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
