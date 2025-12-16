use pyo3::prelude::*;
use regex::Regex;
use scraper::{Html, Selector};
use std::fs::File;
use std::io::{BufReader, Read};
use zip::ZipArchive;

use crate::analysis::analyze;
use rustc_hash::{FxHashMap, FxHashSet};
use std::path::Path;

fn extract_text_from_html(html: &str) -> String {
    let document = Html::parse_document(html);
    let selector = Selector::parse("body").unwrap();
    let mut text = String::new();

    if let Some(body) = document.select(&selector).next() {
        for node in body.text() {
            text.push_str(node);
            text.push(' ');
        }
    } else {
        for node in document.root_element().text() {
            text.push_str(node);
            text.push(' ');
        }
    }

    let whitespace = Regex::new(r"\s+").unwrap();
    whitespace.replace_all(&text, " ").trim().to_string()
}

pub fn extract_json_field(json: &str, field: &str) -> Option<String> {
    // Try with space (Python default) and without
    let patterns = [format!("\"{}\": \"", field), format!("\"{}\":\"", field)];

    for pattern in &patterns {
        if let Some(start_idx) = json.find(pattern) {
            let start = start_idx + pattern.len();
            let rest = &json[start..];
            if let Some(end) = rest.find('"') {
                return Some(rest[..end].to_string());
            }
        }
    }
    None
}

fn should_skip_epub_file(name: &str) -> bool {
    let skip_patterns = [
        "toc",
        "nav",
        "cover",
        "license",
        "gutenberg",
        "copyright",
        "colophon",
    ];
    let lower = name.to_lowercase();
    skip_patterns.iter().any(|p| lower.contains(p))
}

fn parse_epub_internal(path: &str) -> Option<String> {
    let file = File::open(path).ok()?;
    let reader = BufReader::new(file);
    let mut archive = ZipArchive::new(reader).ok()?;

    let mut texts = Vec::new();

    for i in 0..archive.len() {
        let mut file = archive.by_index(i).ok()?;
        let name = file.name().to_lowercase();

        if (name.ends_with(".html") || name.ends_with(".xhtml") || name.ends_with(".htm"))
            && !should_skip_epub_file(&name)
        {
            let mut content = String::new();
            file.read_to_string(&mut content).ok()?;
            let text = extract_text_from_html(&content);
            if !text.is_empty() {
                texts.push(text);
            }
        }
    }

    Some(texts.join(" "))
}

#[pyfunction]
pub fn parse_epub(path: &str) -> Option<String> {
    parse_epub_internal(path)
}

#[pyfunction]
pub fn parse_pdf(path: &str) -> Option<String> {
    let bytes = std::fs::read(path).ok()?;
    let text = pdf_extract::extract_text_from_mem(&bytes).ok()?;
    let whitespace = Regex::new(r"\s+").unwrap();
    Some(whitespace.replace_all(&text, " ").trim().to_string())
}

#[pyfunction]
pub fn parse_txt(path: &str) -> Option<String> {
    let text = std::fs::read_to_string(path).ok()?;
    let whitespace = Regex::new(r"\s+").unwrap();
    Some(whitespace.replace_all(&text, " ").trim().to_string())
}

#[pyfunction]
pub fn chunk_text(text: &str, chunk_size: usize, overlap: usize) -> Vec<String> {
    if text.len() <= chunk_size {
        return if text.is_empty() {
            vec![]
        } else {
            vec![text.to_string()]
        };
    }

    let mut chunks = Vec::new();
    let bytes = text.as_bytes();
    let mut start = 0;

    while start < bytes.len() {
        let mut end = (start + chunk_size).min(bytes.len());

        if end < bytes.len() {
            while end > start && bytes[end] != b' ' {
                end -= 1;
            }
            if end == start {
                end = (start + chunk_size).min(bytes.len());
            }
        }

        if let Ok(chunk) = std::str::from_utf8(&bytes[start..end]) {
            let trimmed = chunk.trim();
            if !trimmed.is_empty() {
                chunks.push(trimmed.to_string());
            }
        }

        start = if end > overlap { end - overlap } else { end };
        if start >= bytes.len() || end == bytes.len() {
            break;
        }
    }

    chunks
}

pub fn parse_file(path: &str) -> Option<String> {
    if path.ends_with(".epub") {
        parse_epub_internal(path)
    } else if path.ends_with(".pdf") {
        let bytes = std::fs::read(path).ok()?;
        let text = pdf_extract::extract_text_from_mem(&bytes).ok()?;
        let whitespace = Regex::new(r"\s+").unwrap();
        Some(whitespace.replace_all(&text, " ").trim().to_string())
    } else if path.ends_with(".txt") {
        let text = std::fs::read_to_string(path).ok()?;
        let whitespace = Regex::new(r"\s+").unwrap();
        Some(whitespace.replace_all(&text, " ").trim().to_string())
    } else {
        None
    }
}

/// Calculate MD5 hashes for multiple files
#[pyfunction]
pub fn file_hashes_batch(_py: Python<'_>, paths: Vec<String>) -> Vec<(String, String)> {
    paths
        .into_iter()
        .filter_map(|path| {
            let data = std::fs::read(&path).ok()?;
            let hash = format!("{:x}", md5::compute(&data));
            Some((path, hash))
        })
        .collect()
}

pub fn process_single_book(
    path: &str,
    book_id: &str,
    hash: &str,
    chunks_dir: &Path,
    stopwords_set: &FxHashSet<String>,
    chunk_size: usize,
    chunk_overlap: usize,
) -> Option<(String, String, u32, FxHashMap<String, Vec<(u32, u32)>>, u64)> {
    // println!("Processing book: {}", book_id);
    let text = parse_file(path)?;
    let chunks = chunk_text(&text, chunk_size, chunk_overlap);
    if chunks.is_empty() {
        return None;
    }

    // Save chunks to zstd
    let shard = if book_id.len() < 2 {
        format!("{:0>2}", book_id)
    } else {
        book_id[..2].to_string()
    };
    let shard_dir = chunks_dir.join(&shard);
    std::fs::create_dir_all(&shard_dir).ok();
    let chunk_path = shard_dir.join(format!("{}.zst", book_id));

    let full_text = chunks.join("\n");
    let compressed = zstd::stream::encode_all(full_text.as_bytes(), 0).ok()?;
    std::fs::write(chunk_path, compressed).ok();

    // Index chunks
    let mut terms: FxHashMap<String, Vec<(u32, u32)>> = FxHashMap::default();
    let mut total_len = 0u64;

    for (local_id, chunk) in chunks.iter().enumerate() {
        let tokens = analyze(chunk);
        total_len += tokens.len() as u64;

        let mut freq_map: FxHashMap<&str, u32> = FxHashMap::default();
        for token in &tokens {
            if !stopwords_set.contains(token) {
                *freq_map.entry(token).or_insert(0) += 1;
            }
        }

        for (term, freq) in freq_map {
            terms
                .entry(term.to_string())
                .or_default()
                .push((local_id as u32, freq));
        }
    }

    Some((
        book_id.to_string(),
        hash.to_string(),
        chunks.len() as u32,
        terms,
        total_len,
    ))
}
