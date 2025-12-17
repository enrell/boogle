use pyo3::prelude::*;

mod analysis;
mod codecs;
mod document;
mod index;
mod pipeline;
mod search;
mod store;
mod util;

use analysis::analyze;
use codecs::{decode_postings, encode_postings, merge_postings};
use document::parsers::{chunk_text, file_hashes_batch, parse_epub, parse_pdf, parse_txt};
use index::memory::{process_batch, process_books_to_index, BM25Index};
use index::writer::index_corpus_file;
use pipeline::run_streaming_pipeline;
use search::searcher::FileSearcher;
use search::wand::WandSearcher;

#[pymodule]
fn rust_bm25(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<BM25Index>()?;
    m.add_class::<WandSearcher>()?;
    m.add_class::<FileSearcher>()?;
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
    m.add_function(wrap_pyfunction!(index_corpus_file, m)?)?;
    m.add_function(wrap_pyfunction!(run_streaming_pipeline, m)?)?;
    Ok(())
}
