use once_cell::sync::Lazy;
use pyo3::prelude::*;
use rust_stemmers::{Algorithm, Stemmer};

// Lazy-initialized stemmer for Portuguese
// Using Portuguese since most book content appears to be in Portuguese
static STEMMER: Lazy<Stemmer> = Lazy::new(|| Stemmer::create(Algorithm::Portuguese));

#[pyfunction]
pub fn analyze(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_alphabetic())
        .filter(|s| s.len() >= 2 && s.len() <= 30)
        .map(|s| STEMMER.stem(s).into_owned())
        .collect()
}
