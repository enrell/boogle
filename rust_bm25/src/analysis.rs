use deunicode::deunicode;
use once_cell::sync::Lazy;
use pyo3::prelude::*;
use rust_stemmers::{Algorithm, Stemmer};

static STEMMER: Lazy<Stemmer> = Lazy::new(|| Stemmer::create(Algorithm::Portuguese));

#[pyfunction]
pub fn analyze(text: &str) -> Vec<String> {
    let ascii_text = deunicode(text);

    ascii_text
        .to_lowercase()
        .split(|c: char| !c.is_ascii_alphabetic())
        .filter(|s| s.len() >= 2 && s.len() <= 25)
        .map(|s| STEMMER.stem(s).into_owned())
        .collect()
}

use bumpalo::Bump;
use std::borrow::Cow;

#[inline]
pub fn analyze_arena<'a>(text: &str, bump: &'a Bump) -> Vec<&'a str> {
    let ascii_text = deunicode(text);

    // We allocate the lowercase ascii string in the arena
    let lower = ascii_text.to_lowercase();
    let lower_ref = bump.alloc_str(&lower);

    lower_ref
        .split(|c: char| !c.is_ascii_alphabetic())
        .filter(|s| {
            let len = s.len();
            len >= 2 && len <= 25
        })
        .map(|s| {
            let stemmed = STEMMER.stem(s);
            match stemmed {
                Cow::Borrowed(b) => b,
                Cow::Owned(o) => bump.alloc_str(&o),
            }
        })
        .collect()
}
