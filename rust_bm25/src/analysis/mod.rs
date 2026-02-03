use bumpalo::Bump;
use deunicode::deunicode;
use once_cell::sync::Lazy;
use pyo3::prelude::*;
use rust_stemmers::{Algorithm, Stemmer};
use std::borrow::Cow;

static STEMMER: Lazy<Stemmer> = Lazy::new(|| Stemmer::create(Algorithm::Portuguese));

const MIN_TOKEN_LEN: usize = 2;
const MAX_TOKEN_LEN: usize = 25;

#[pyfunction]
pub fn analyze(text: &str) -> Vec<String> {
    deunicode(text)
        .to_lowercase()
        .split(|c: char| !c.is_ascii_alphabetic())
        .filter(|s| (MIN_TOKEN_LEN..=MAX_TOKEN_LEN).contains(&s.len()))
        .map(|s| STEMMER.stem(s).into_owned())
        .collect()
}

#[inline]
pub fn analyze_arena<'a>(text: &str, bump: &'a Bump) -> Vec<&'a str> {
    let ascii = deunicode(text);
    let lower = bump.alloc_str(&ascii.to_lowercase());

    lower
        .split(|c: char| !c.is_ascii_alphabetic())
        .filter(|s| (MIN_TOKEN_LEN..=MAX_TOKEN_LEN).contains(&s.len()))
        .map(|s| match STEMMER.stem(s) {
            Cow::Borrowed(b) => b,
            Cow::Owned(o) => bump.alloc_str(&o),
        })
        .collect()
}
