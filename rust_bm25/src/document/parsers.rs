use once_cell::sync::Lazy;
use pyo3::prelude::*;
use scraper::{Html, Selector};
use std::fs::File;
use std::io::{BufReader, Cursor, Read, Seek};
use zip::ZipArchive;

static BODY_SELECTOR: Lazy<Selector> = Lazy::new(|| Selector::parse("body").unwrap());

const EPUB_SKIP_PATTERNS: [&str; 7] = [
    "toc",
    "nav",
    "cover",
    "license",
    "gutenberg",
    "copyright",
    "colophon",
];

#[pyfunction]
pub fn parse_epub(path: &str) -> Option<String> {
    let file = File::open(path).ok()?;
    parse_epub_from_reader(BufReader::new(file))
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
    Some(normalize_whitespace(unsafe {
        &String::from_utf8_unchecked(bytes)
    }))
}

pub fn parse_file(path: &str) -> Option<String> {
    match path.rsplit('.').next()? {
        "epub" => parse_epub(path),
        "pdf" => parse_pdf(path),
        "txt" => parse_txt(path),
        _ => None,
    }
}

pub fn parse_bytes(bytes: &[u8], extension: &str) -> Option<String> {
    match extension {
        "epub" => parse_epub_from_reader(Cursor::new(bytes)),
        "pdf" => Some(normalize_whitespace(
            &pdf_extract::extract_text_from_mem(bytes).ok()?,
        )),
        "txt" => {
            simdutf8::basic::from_utf8(bytes).ok()?;
            Some(normalize_whitespace(unsafe {
                &String::from_utf8_unchecked(bytes.to_vec())
            }))
        }
        _ => None,
    }
}

fn parse_epub_from_reader<R: Read + Seek>(reader: R) -> Option<String> {
    let mut archive = ZipArchive::new(reader).ok()?;
    let mut texts = Vec::new();

    for i in 0..archive.len() {
        let mut file = archive.by_index(i).ok()?;
        let name = file.name().to_lowercase();

        if is_html_file(&name) && !should_skip(&name) {
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

fn is_html_file(name: &str) -> bool {
    name.ends_with(".html") || name.ends_with(".xhtml") || name.ends_with(".htm")
}

fn should_skip(name: &str) -> bool {
    EPUB_SKIP_PATTERNS.iter().any(|p| name.contains(p))
}

fn extract_text_from_html(html: &str) -> String {
    let document = Html::parse_document(html);
    let mut text = String::new();

    let elements = document
        .select(&BODY_SELECTOR)
        .next()
        .map(|b| b.text())
        .unwrap_or_else(|| document.root_element().text());

    for node in elements {
        text.push_str(node);
        text.push(' ');
    }

    normalize_whitespace(&text)
}

fn normalize_whitespace(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let mut prev_space = true;

    for c in text.chars() {
        if c.is_whitespace() {
            if !prev_space {
                result.push(' ');
                prev_space = true;
            }
        } else {
            result.push(c);
            prev_space = false;
        }
    }

    if result.ends_with(' ') {
        result.pop();
    }

    result
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
    let mut start_idx = 0;

    while start_idx < total_chars {
        let mut end_idx = (start_idx + chunk_size).min(total_chars);

        if end_idx < total_chars {
            end_idx = find_word_boundary(text, &char_indices, start_idx, end_idx);
        }

        let start_byte = char_indices[start_idx];
        let end_byte = if end_idx == total_chars {
            text.len()
        } else {
            char_indices[end_idx]
        };

        let chunk = text[start_byte..end_byte].trim();
        if !chunk.is_empty() {
            chunks.push(chunk.to_string());
        }

        let advance = if end_idx > overlap {
            end_idx - overlap
        } else {
            end_idx
        };

        if advance <= start_idx {
            start_idx = end_idx;
        } else {
            start_idx = advance;
        }

        if end_idx >= total_chars {
            break;
        }
    }

    chunks
}

fn find_word_boundary(text: &str, indices: &[usize], start: usize, end: usize) -> usize {
    let search_limit = (end.saturating_sub(100)).max(start);

    for i in (search_limit..end).rev() {
        let byte_idx = indices[i];
        if text[byte_idx..].chars().next() == Some(' ') {
            return i;
        }
    }

    end
}

#[allow(dead_code)]
pub fn extract_json_field(json: &str, field: &str) -> Option<String> {
    let patterns = [format!("\"{}\": \"", field), format!("\"{}\":\"", field)];

    for pattern in &patterns {
        if let Some(start_idx) = json.find(pattern) {
            let start = start_idx + pattern.len();
            if let Some(end) = json[start..].find('"') {
                return Some(json[start..start + end].to_string());
            }
        }
    }
    None
}

#[pyfunction]
pub fn file_hashes_batch(_py: Python<'_>, paths: Vec<String>) -> Vec<(String, String)> {
    paths
        .into_iter()
        .filter_map(|path| {
            let data = std::fs::read(&path).ok()?;
            Some((path, format!("{:x}", md5::compute(&data))))
        })
        .collect()
}
