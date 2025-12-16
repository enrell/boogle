use pyo3::prelude::*;

mod analysis;
mod encoding;
mod memory_index;
mod parsers;
mod postgres_index;
mod search;

mod sqlite_index;

use analysis::analyze;
use encoding::{decode_postings, encode_postings, merge_postings};
use memory_index::{process_batch, process_books_to_index, BM25Index};
use parsers::{chunk_text, file_hashes_batch, parse_epub, parse_pdf, parse_txt};
use postgres_index::index_corpus;
use search::WandSearcher;
use sqlite_index::index_corpus_sqlite;

#[pymodule]
fn rust_bm25(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<BM25Index>()?;
    m.add_class::<WandSearcher>()?;
    m.add_function(wrap_pyfunction!(analyze, m)?)?;
    m.add_function(wrap_pyfunction!(encode_postings, m)?)?;
    m.add_function(wrap_pyfunction!(decode_postings, m)?)?;
    m.add_function(wrap_pyfunction!(merge_postings, m)?)?;
    m.add_function(wrap_pyfunction!(parse_epub, m)?)?;
    m.add_function(wrap_pyfunction!(parse_pdf, m)?)?;
    m.add_function(wrap_pyfunction!(parse_txt, m)?)?;
    m.add_function(wrap_pyfunction!(chunk_text, m)?)?;
    m.add_function(wrap_pyfunction!(process_books_to_index, m)?)?;
    m.add_function(wrap_pyfunction!(process_batch, m)?)?;
    m.add_function(wrap_pyfunction!(file_hashes_batch, m)?)?;
    m.add_function(wrap_pyfunction!(index_corpus, m)?)?;
    m.add_function(wrap_pyfunction!(index_corpus_sqlite, m)?)?;
    Ok(())
}
