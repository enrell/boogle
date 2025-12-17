use pyo3::prelude::*;
use r2d2::Pool as R2Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tokio::runtime::Runtime;

use crate::codecs::{decode_postings_internal, encode_postings_internal};
use crate::document::parsers::process_single_book; // Direct import as it was used directly
use rusqlite::OptionalExtension;

#[pyfunction]
#[pyo3(signature = (books_dir, chunks_dir, db_path, stopwords, chunk_size=1000, chunk_overlap=100, full=false, batch_size=100))]
pub fn index_corpus_sqlite(
    py: Python<'_>,
    books_dir: String,
    chunks_dir: String,
    db_path: String,
    stopwords: Vec<String>,
    chunk_size: usize,
    chunk_overlap: usize,
    full: bool,
    batch_size: usize,
) -> PyResult<(u32, u32, u32)> {
    let stopwords_set: FxHashSet<String> = stopwords.into_iter().collect();

    py.detach(|| {
        let rt =
            Runtime::new().map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        rt.block_on(async {
            index_corpus_sqlite_async(
                &books_dir,
                &chunks_dir,
                &db_path,
                &stopwords_set,
                chunk_size,
                chunk_overlap,
                full,
                batch_size,
            )
            .await
        })
    })
}

async fn index_corpus_sqlite_async(
    books_dir: &str,
    chunks_dir: &str,
    db_path: &str,
    stopwords_set: &FxHashSet<String>,
    chunk_size: usize,
    chunk_overlap: usize,
    full: bool,
    batch_size: usize,
) -> PyResult<(u32, u32, u32)> {
    let manager = SqliteConnectionManager::file(db_path);
    let pool = R2Pool::new(manager)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("DB pool error: {}", e)))?;
    let pool = Arc::new(pool); // Shareable across threads

    {
        let pool = pool.clone();
        tokio::task::spawn_blocking(move || {
            let conn = pool.get().map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            init_schema(&conn).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            if full {
                // Clear index logic would go here
                 conn.execute_batch("DELETE FROM idx_chunks; DELETE FROM idx_terms; DELETE FROM idx_globals; DELETE FROM idx_books_indexed;").map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            }
            Ok::<(), PyErr>(())
        }).await.map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))??;
    }

    // Reuse glob/hash logic from postgres_index.rs?
    // It's better to refactor `postgres_index.rs` to share `get_files_and_hashes`
    // but for now I will duplicate the glob/hash part to save time and avoid creating a new file.
    use glob::glob;

    let start_glob = Instant::now();
    let patterns = [
        format!("{}/*.epub", books_dir),
        format!("{}/*.txt", books_dir),
        format!("{}/*.pdf", books_dir),
    ];

    let mut book_files: Vec<String> = Vec::new();
    for pattern in &patterns {
        for entry in
            glob(pattern).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
        {
            if let Ok(path) = entry {
                book_files.push(path.to_string_lossy().to_string());
            }
        }
    }
    println!("Glob took {:?}", start_glob.elapsed());

    // Hash
    println!("Computing file hashes...");
    let start_hash = Instant::now();
    let hashes: Vec<(String, String, String)> = tokio::task::spawn_blocking(move || {
        book_files
            .par_iter()
            .filter_map(|path| {
                let data = std::fs::read(path).ok()?;
                let hash = format!("{:x}", md5::compute(&data));
                let book_id = Path::new(path).file_stem()?.to_string_lossy().to_string();
                Some((path.clone(), book_id, hash))
            })
            .collect()
    })
    .await
    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    println!("Hashing took {:?}", start_hash.elapsed());

    let mut files_to_process = Vec::new();
    let mut skipped = 0;

    if !full {
        let hash_ids: Vec<String> = hashes.iter().map(|(_, id, _)| id.clone()).collect();
        let pool = pool.clone();
        let indexed_map = tokio::task::spawn_blocking(move || {
            let conn = pool
                .get()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            get_indexed_books(&conn, &hash_ids)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
        .await
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))??;

        for (path, book_id, hash) in hashes {
            if let Some(existing_hash) = indexed_map.get(&book_id) {
                if existing_hash == &hash {
                    skipped += 1;
                    continue;
                }
            }
            files_to_process.push((path, book_id, hash));
        }
        println!("Skipped {} already indexed books", skipped);
    } else {
        files_to_process = hashes;
    }

    let mut indexed = 0u32;

    let next_chunk_id = {
        let pool = pool.clone();
        tokio::task::spawn_blocking(move || {
            let conn = pool
                .get()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            get_next_chunk_id(&conn)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
        .await
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))??
    };
    let mut current_chunk_id = next_chunk_id;

    // Pipelining
    let mut last_db_task: Option<tokio::task::JoinHandle<PyResult<()>>> = None;

    for batch_start in (0..files_to_process.len()).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(files_to_process.len());
        let batch: Vec<_> = files_to_process[batch_start..batch_end]
            .iter()
            .cloned()
            .collect();

        let stopwords_clone = stopwords_set.clone();
        let chunks_dir_clone = chunks_dir.to_string();
        let chunk_size = chunk_size;
        let chunk_overlap = chunk_overlap;

        let start_cpu = Instant::now();
        let batch_results = tokio::task::spawn_blocking(move || {
            let chunks_dir_path = Path::new(&chunks_dir_clone);
            batch
                .par_iter()
                .filter_map(|(path, book_id, hash)| {
                    process_single_book(
                        path,
                        book_id,
                        hash,
                        chunks_dir_path,
                        &stopwords_clone,
                        chunk_size,
                        chunk_overlap,
                    )
                })
                .collect::<Vec<_>>()
        })
        .await
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        println!("Batch CPU processing took {:?}", start_cpu.elapsed());

        let mut batch_chunks = Vec::new();
        let mut batch_terms: FxHashMap<String, Vec<(u32, u32)>> = FxHashMap::default();
        let mut batch_books = Vec::new();

        for (book_id, hash, chunks_count, terms, _) in batch_results {
            let start_id = current_chunk_id;
            for i in 0..chunks_count {
                batch_chunks.push(((start_id + i) as i32, book_id.clone()));
            }
            for (term, postings) in terms {
                let offset_postings: Vec<(u32, u32)> = postings
                    .into_iter()
                    .map(|(local_id, freq)| (start_id + local_id, freq))
                    .collect();
                batch_terms.entry(term).or_default().extend(offset_postings);
            }
            batch_books.push((book_id, hash, chunks_count as i32));
            current_chunk_id += chunks_count;
        }

        if let Some(task) = last_db_task.take() {
            task.await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))??;
        }

        let pool_clone = pool.clone();
        let do_merge = !full || indexed > 0;
        let batch_books_len = batch_books.len();

        last_db_task = Some(tokio::task::spawn_blocking(move || {
            let mut conn = pool_clone
                .get()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            let start_db = Instant::now();
            let tx = conn
                .transaction()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            insert_chunks(&tx, &batch_chunks)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            insert_terms(&tx, batch_terms, do_merge)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            mark_books_indexed(&tx, &batch_books)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            tx.commit()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            println!("Batch DB insertion took {:?}", start_db.elapsed());
            Ok::<(), PyErr>(())
        }));

        indexed += batch_books_len as u32;
        println!(
            "Indexed {} books, {} total chunks",
            indexed, current_chunk_id
        );
    }

    if let Some(task) = last_db_task.take() {
        task.await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))??;
    }

    // Globals update - simplistic for now
    let pool_clone = pool.clone();
    tokio::task::spawn_blocking(move || {
        let conn = pool_clone
            .get()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        set_global(&conn, "num_docs", current_chunk_id as f64)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok::<(), PyErr>(())
    })
    .await
    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))??;

    Ok((indexed, skipped, current_chunk_id))
}

// Helpers relying on rusqlite::Connection or Transaction

fn init_schema(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        r#"
    CREATE TABLE IF NOT EXISTS idx_chunks (
        chunk_id INTEGER PRIMARY KEY,
        book_id TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS idx_terms (
        term TEXT PRIMARY KEY,
        df INTEGER NOT NULL,
        postings BLOB NOT NULL
    );
    CREATE TABLE IF NOT EXISTS idx_globals (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS idx_books_indexed (
        book_id TEXT PRIMARY KEY,
        file_hash TEXT NOT NULL,
        chunk_count INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_chunks_book ON idx_chunks(book_id);
    "#,
    )
}

fn get_next_chunk_id(conn: &rusqlite::Connection) -> rusqlite::Result<u32> {
    let mut stmt = conn.prepare("SELECT COALESCE(MAX(chunk_id), -1) + 1 FROM idx_chunks")?;
    let id: i32 = stmt.query_row([], |row| row.get(0)).unwrap_or(0);
    Ok(id as u32)
}

fn get_indexed_books(
    conn: &rusqlite::Connection,
    book_ids: &[String],
) -> rusqlite::Result<FxHashMap<String, String>> {
    // Rusqlite doesn't support ANY($1) easily. We must query in batches or loop.
    // Simplified: Select all matching or loop.
    // For performance, better to use rarray or temp table, but for now just loop with prepared statement or WHERE IN
    if book_ids.is_empty() {
        return Ok(FxHashMap::default());
    }

    let mut result = FxHashMap::default();

    // Chunking to avoid too many params
    for chunk in book_ids.chunks(500) {
        let params_str = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let sql = format!(
            "SELECT book_id, file_hash FROM idx_books_indexed WHERE book_id IN ({})",
            params_str
        );
        let mut stmt = conn.prepare(&sql)?;

        let rows = stmt.query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;

        for r in rows {
            if let Ok((id, hash)) = r {
                result.insert(id, hash);
            }
        }
    }
    Ok(result)
}

fn insert_chunks(tx: &rusqlite::Transaction, chunks: &[(i32, String)]) -> rusqlite::Result<()> {
    let mut stmt =
        tx.prepare("INSERT OR IGNORE INTO idx_chunks (chunk_id, book_id) VALUES (?, ?)")?;
    for (cid, bid) in chunks {
        stmt.execute(rusqlite::params![cid, bid])?;
    }
    Ok(())
}

fn insert_terms(
    tx: &rusqlite::Transaction,
    terms: FxHashMap<String, Vec<(u32, u32)>>,
    merge: bool,
) -> rusqlite::Result<()> {
    // Encoding can still be parallelized
    let term_list: Vec<(String, i32, Vec<u8>)> = if !merge {
        terms
            .into_par_iter()
            .map(|(term, postings)| {
                (
                    term,
                    postings.len() as i32,
                    encode_postings_internal(&postings),
                )
            })
            .collect()
    } else {
        // Need existing terms.
        // For now, simpler sync implementation inside db task:
        let mut list = Vec::new();
        for (term, postings) in terms {
            // Fetch existing
            let existing: Option<(i32, Vec<u8>)> = tx
                .query_row(
                    "SELECT df, postings FROM idx_terms WHERE term = ?",
                    [&term],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;

            let (df, encoded) = if let Some((old_df, old_postings)) = existing {
                let mut merged = decode_postings_internal(&old_postings);
                merged.extend(postings.iter().cloned());
                (
                    old_df + postings.len() as i32,
                    encode_postings_internal(&merged),
                )
            } else {
                (postings.len() as i32, encode_postings_internal(&postings))
            };
            list.push((term, df, encoded));
        }
        list
    };

    let mut stmt =
        tx.prepare("INSERT OR REPLACE INTO idx_terms (term, df, postings) VALUES (?, ?, ?)")?;
    for (t, d, p) in term_list {
        stmt.execute(rusqlite::params![t, d, p])?;
    }
    Ok(())
}

fn mark_books_indexed(
    tx: &rusqlite::Transaction,
    books: &[(String, String, i32)],
) -> rusqlite::Result<()> {
    let mut stmt = tx.prepare("INSERT OR REPLACE INTO idx_books_indexed (book_id, file_hash, chunk_count) VALUES (?, ?, ?)")?;
    for (bid, fh, cc) in books {
        stmt.execute(rusqlite::params![bid, fh, cc])?;
    }
    Ok(())
}

fn set_global(conn: &rusqlite::Connection, key: &str, value: f64) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO idx_globals (key, value) VALUES (?, ?)",
        rusqlite::params![key, value.to_string()],
    )?;
    Ok(())
}
