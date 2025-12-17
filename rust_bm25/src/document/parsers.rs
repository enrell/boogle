use pyo3::prelude::*;
use scraper::{Html, Selector};
use std::fs::File;
use std::io::{BufReader, Cursor, Read, Seek};
use zip::ZipArchive;

use once_cell::sync::Lazy;

// Pre-compiled selector for better performance
static BODY_SELECTOR: Lazy<Selector> = Lazy::new(|| Selector::parse("body").unwrap());

#[inline]
fn normalize_whitespace(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let mut prev_was_space = true; // Start true to trim leading

    for c in text.chars() {
        if c.is_whitespace() {
            if !prev_was_space {
                result.push(' ');
                prev_was_space = true;
            }
        } else {
            result.push(c);
            prev_was_space = false;
        }
    }

    // Trim trailing space if present
    if result.ends_with(' ') {
        result.pop();
    }

    result
}

fn extract_text_from_html(html: &str) -> String {
    let document = Html::parse_document(html);
    let mut text = String::new();

    if let Some(body) = document.select(&BODY_SELECTOR).next() {
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

    normalize_whitespace(&text)
}

#[allow(dead_code)]
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

fn parse_epub_from_reader<R: Read + Seek>(reader: R) -> Option<String> {
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

fn parse_epub_internal(path: &str) -> Option<String> {
    let file = File::open(path).ok()?;
    let reader = BufReader::new(file);
    parse_epub_from_reader(reader)
}

pub fn parse_bytes(bytes: &[u8], extension: &str) -> Option<String> {
    if extension == "epub" {
        let cursor = Cursor::new(bytes);
        parse_epub_from_reader(cursor)
    } else if extension == "pdf" {
        let text = pdf_extract::extract_text_from_mem(bytes).ok()?;
        Some(normalize_whitespace(&text))
    } else if extension == "txt" {
        if simdutf8::basic::from_utf8(bytes).is_err() {
            return None;
        }
        let text = unsafe { String::from_utf8_unchecked(bytes.to_vec()) };
        Some(normalize_whitespace(&text))
    } else {
        None
    }
}

#[pyfunction]
pub fn parse_epub(path: &str) -> Option<String> {
    parse_epub_internal(path)
}

#[pyfunction]
pub fn parse_pdf(path: &str) -> Option<String> {
    let bytes = std::fs::read(path).ok()?;
    let text = pdf_extract::extract_text_from_mem(&bytes).ok()?;
    Some(normalize_whitespace(&text))
}

#[pyfunction]
pub fn parse_txt(path: &str) -> Option<String> {
    let bytes = std::fs::read(path).ok()?;
    if simdutf8::basic::from_utf8(&bytes).is_err() {
        return None;
    }
    let text = unsafe { String::from_utf8_unchecked(bytes) };
    Some(normalize_whitespace(&text))
}

#[pyfunction]
pub fn chunk_text(text: &str, chunk_size: usize, overlap: usize) -> Vec<String> {
    if text.is_empty() {
        return vec![];
    }

    let char_indices: Vec<usize> = text.char_indices().map(|(i, _)| i).collect();
    let total_chars = char_indices.len();

    if total_chars <= chunk_size {
        let trimmed = text.trim();
        return if trimmed.is_empty() {
            vec![]
        } else {
            vec![trimmed.to_string()]
        };
    }

    let mut chunks = Vec::new();
    let mut start_char_idx = 0;

    while start_char_idx < total_chars {
        let mut end_char_idx = (start_char_idx + chunk_size).min(total_chars);

        // Try to break at word boundary (space) if not at end
        if end_char_idx < total_chars {
            let mut best_break = end_char_idx;
            // Look backwards for a space, limit search to last 100 chars or so to avoid scanning too much
            let search_limit = (end_char_idx.saturating_sub(100)).max(start_char_idx);

            for i in (search_limit..end_char_idx).rev() {
                let byte_idx = char_indices[i];
                // Check if the character at this byte index is a space
                if text[byte_idx..].chars().next() == Some(' ') {
                    best_break = i;
                    break;
                }
            }
            if best_break > start_char_idx {
                end_char_idx = best_break;
            }
        }

        // Slice string using byte indices
        let start_byte = char_indices[start_char_idx];
        let end_byte = if end_char_idx == total_chars {
            text.len()
        } else {
            char_indices[end_char_idx]
        };

        let chunk = &text[start_byte..end_byte];
        let trimmed = chunk.trim();
        if !trimmed.is_empty() {
            chunks.push(trimmed.to_string());
        }

        // Move forward with overlap
        let advance_char_idx = if end_char_idx > overlap {
            end_char_idx - overlap
        } else {
            end_char_idx
        };

        if advance_char_idx <= start_char_idx {
            // Force progress
            start_char_idx = end_char_idx;
        } else {
            start_char_idx = advance_char_idx;
        }

        if end_char_idx >= total_chars {
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
        Some(normalize_whitespace(&text))
    } else if path.ends_with(".txt") {
        let bytes = std::fs::read(path).ok()?;
        if simdutf8::basic::from_utf8(&bytes).is_err() {
            return None;
        }
        let text = unsafe { String::from_utf8_unchecked(bytes) };
        Some(normalize_whitespace(&text))
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
