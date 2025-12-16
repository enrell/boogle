use pyo3::prelude::*;

#[pyfunction]
pub fn analyze(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_alphabetic())
        .filter(|s| s.len() >= 2 && s.len() <= 30)
        .map(String::from)
        .collect()
}
