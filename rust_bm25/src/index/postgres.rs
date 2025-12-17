use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use glob::glob;
use pyo3::prelude::*;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use std::path::Path;
use std::time::Instant;
use tokio::runtime::Runtime;

use crate::analysis::analyze;
use crate::codecs::{decode_postings_internal, encode_postings_internal};
use crate::document::parsers::{chunk_text, parse_file};

/// Main indexing function - does everything in Rust
#[pyfunction]
#[pyo3(signature = (books_dir, chunks_dir, db_url, stopwords, chunk_size=1000, chunk_overlap=100, full=false, batch_size=100))]
pub fn index_corpus(
    py: Python<'_>,
    books_dir: String,
    chunks_dir: String,
    db_url: String,
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
            index_corpus_async(
                &books_dir,
                &chunks_dir,
                &db_url,
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

async fn index_corpus_async(
    books_dir: &str,
    chunks_dir: &str,
    db_url: &str,
    stopwords_set: &FxHashSet<String>,
    chunk_size: usize,
    chunk_overlap: usize,
    full: bool,
    batch_size: usize,
) -> PyResult<(u32, u32, u32)> {
    let manager = PostgresConnectionManager::new_from_stringlike(db_url, tokio_postgres::NoTls)
        .map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("DB config error: {}", e))
        })?;

    let pool = Pool::builder()
        .max_size(20)
        .build(manager)
        .await
        .map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("DB pool create error: {}", e))
        })?;

    {
        let conn = pool.get().await.map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("DB connection error: {}", e))
        })?;
        init_schema(&conn).await?;

        if full {
            clear_index(&conn, chunks_dir).await?;
        }
    }

    let next_chunk_id = {
        let conn = pool.get().await.map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("DB connection error: {}", e))
        })?;
        get_next_chunk_id(&conn).await?
    };

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

    let total_files = book_files.len();
    println!("Found {} book files", total_files);

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

    let mut files_to_process: Vec<(String, String, String)> = Vec::new();
    let mut skipped = 0u32;

    if !full {
        println!("Checking already indexed books...");
        let start_check = Instant::now();
        let conn = pool.get().await.map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("DB connection error: {}", e))
        })?;
        let indexed = get_indexed_books(
            &conn,
            &hashes
                .iter()
                .map(|(_, id, _)| id.clone())
                .collect::<Vec<_>>(),
        )
        .await?;
        println!("Checking indexed books took {:?}", start_check.elapsed());

        for (path, book_id, hash) in hashes {
            if let Some(existing_hash) = indexed.get(&book_id) {
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

    println!("Processing {} books...", files_to_process.len());

    let mut indexed = 0u32;
    let mut current_chunk_id = next_chunk_id;
    let mut total_length = 0u64;

    let mut last_db_task: Option<tokio::task::JoinHandle<PyResult<()>>> = None;

    for batch_start in (0..files_to_process.len()).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(files_to_process.len());
        let batch: Vec<_> = files_to_process[batch_start..batch_end]
            .iter()
            .cloned()
            .collect();

        println!(
            "Processing batch {}-{} of {}...",
            batch_start,
            batch_end,
            files_to_process.len()
        );

        // Process entire batch in parallel using rayon inside spawn_blocking
        let stopwords_clone = stopwords_set.clone();
        let chunks_dir_clone = chunks_dir.to_string();

        let start_cpu = Instant::now();
        let batch_results = tokio::task::spawn_blocking(move || {
            let chunks_dir_path = Path::new(&chunks_dir_clone);
            let res = batch
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
                .collect::<Vec<_>>();
            res
        })
        .await
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        println!("Batch CPU processing took {:?}", start_cpu.elapsed());

        let mut batch_chunks: Vec<(i32, String)> = Vec::new();
        let mut batch_terms: FxHashMap<String, Vec<(u32, u32)>> = FxHashMap::default();
        let mut batch_books: Vec<(String, String, i32)> = Vec::new();
        let mut batch_len = 0u64;

        for (book_id, hash, chunks_count, terms, length) in batch_results {
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
            batch_len += length;
            current_chunk_id += chunks_count;
        }

        // Wait for previous DB task to complete before starting the next one
        if let Some(task) = last_db_task.take() {
            task.await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))??;
        }

        let pool_clone = pool.clone();
        let do_merge = !full || indexed > 0;

        let batch_books_len = batch_books.len();
        last_db_task = Some(tokio::spawn(async move {
            let mut conn = pool_clone.get().await.map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("DB connection error: {}", e))
            })?;

            let start_db = Instant::now();
            let transaction = conn
                .transaction()
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            if !batch_chunks.is_empty() {
                insert_chunks(&transaction, &batch_chunks).await?;
            }

            if !batch_terms.is_empty() {
                insert_terms(&transaction, batch_terms, do_merge).await?;
            }

            if !batch_books.is_empty() {
                mark_books_indexed(&transaction, &batch_books).await?;
            }

            transaction
                .commit()
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            println!("Batch DB insertion took {:?}", start_db.elapsed());

            Ok::<(), PyErr>(())
        }));

        total_length += batch_len;
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

    let new_num = current_chunk_id;
    let old_total: f64 = if full {
        0.0
    } else {
        let conn = pool.get().await.map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("DB connection error: {}", e))
        })?;
        get_global(&conn, "total_length").await?.unwrap_or(0.0)
    };
    let new_total = old_total + total_length as f64;
    let avgdl = if new_num > 0 {
        new_total / new_num as f64
    } else {
        0.0
    };

    let conn = pool.get().await.map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("DB connection error: {}", e))
    })?;
    set_global(&conn, "num_docs", new_num as f64).await?;
    set_global(&conn, "total_length", new_total).await?;
    set_global(&conn, "avgdl", avgdl).await?;
    set_global(&conn, "k1", 1.5).await?;
    set_global(&conn, "b", 0.75).await?;

    println!(
        "Done: {} indexed, {} skipped, {} total chunks",
        indexed, skipped, current_chunk_id
    );

    Ok((indexed, skipped, current_chunk_id))
}

fn process_single_book(
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
    let compressed = zstd::stream::encode_all(full_text.as_bytes(), 3).ok()?;
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

async fn init_schema(client: &tokio_postgres::Client) -> PyResult<()> {
    client
        .batch_execute(
            r#"
        CREATE TABLE IF NOT EXISTS idx_chunks (
            chunk_id INTEGER PRIMARY KEY,
            book_id TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS idx_terms (
            term TEXT PRIMARY KEY,
            df INTEGER NOT NULL,
            postings BYTEA NOT NULL
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
        .await
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    Ok(())
}

async fn clear_index(client: &tokio_postgres::Client, chunks_dir: &str) -> PyResult<()> {
    client
        .batch_execute("TRUNCATE idx_chunks, idx_terms, idx_globals, idx_books_indexed")
        .await
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

    if let Ok(entries) = std::fs::read_dir(chunks_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                std::fs::remove_dir_all(&path).ok();
            } else if path.extension().map_or(false, |e| e == "zst") {
                std::fs::remove_file(&path).ok();
            }
        }
    }
    Ok(())
}

async fn get_next_chunk_id(client: &tokio_postgres::Client) -> PyResult<u32> {
    let row = client
        .query_one(
            "SELECT COALESCE(MAX(chunk_id), -1) + 1 as next_id FROM idx_chunks",
            &[],
        )
        .await
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    let next_id: i32 = row.get(0);
    Ok(next_id as u32)
}

async fn get_indexed_books(
    client: &tokio_postgres::Client,
    book_ids: &[String],
) -> PyResult<FxHashMap<String, String>> {
    if book_ids.is_empty() {
        return Ok(FxHashMap::default());
    }

    let rows = client
        .query(
            "SELECT book_id, file_hash FROM idx_books_indexed WHERE book_id = ANY($1)",
            &[&book_ids],
        )
        .await
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

    let mut result = FxHashMap::default();
    for row in rows {
        let book_id: String = row.get(0);
        let file_hash: String = row.get(1);
        result.insert(book_id, file_hash);
    }
    Ok(result)
}

async fn insert_chunks(
    client: &tokio_postgres::Transaction<'_>,
    chunks: &[(i32, String)],
) -> PyResult<()> {
    for batch in chunks.chunks(1000) {
        let mut query = String::from("INSERT INTO idx_chunks (chunk_id, book_id) VALUES ");
        let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            Vec::with_capacity(batch.len() * 2);

        for (i, (chunk_id, book_id)) in batch.iter().enumerate() {
            if i > 0 {
                query.push(',');
            }
            query.push_str(&format!("(${}, ${})", i * 2 + 1, i * 2 + 2));
            params.push(chunk_id);
            params.push(book_id);
        }

        query.push_str(" ON CONFLICT DO NOTHING");
        client
            .execute(&query, &params)
            .await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    }
    Ok(())
}

async fn insert_terms(
    client: &tokio_postgres::Transaction<'_>,
    terms: FxHashMap<String, Vec<(u32, u32)>>,
    merge: bool,
) -> PyResult<()> {
    let term_list: Vec<(String, i32, Vec<u8>)> = if !merge {
        // Parallel encoding for new terms
        tokio::task::spawn_blocking(move || {
            terms
                .into_par_iter()
                .map(|(term, postings)| {
                    let df = postings.len() as i32;
                    let encoded = encode_postings_internal(&postings);
                    (term, df, encoded)
                })
                .collect()
        })
        .await
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
    } else {
        let term_names: Vec<String> = terms.keys().cloned().collect();
        // Since get_existing_terms expects &[&String], we have to be careful with lifetimes.
        // We need to fetch existing terms first.
        let existing = {
            let refs: Vec<&String> = term_names.iter().collect();
            get_existing_terms(client, &refs).await?
        };

        // Offload merging to thread pool
        tokio::task::spawn_blocking(move || {
            terms
                .into_par_iter()
                .map(|(term, postings)| {
                    let (df, encoded) = if let Some((old_df, old_postings)) = existing.get(&term) {
                        let mut merged = decode_postings_internal(old_postings);
                        merged.extend(postings.iter().cloned());
                        // Important: sort by doc_id if not sorted by logic (though they should be appended in order)
                        // However, decode_postings_internal returns sorted.
                        // And new postings are from new batches which have higher doc_ids.
                        // So simple extend is fine.
                        (
                            old_df + postings.len() as i32,
                            encode_postings_internal(&merged),
                        )
                    } else {
                        (postings.len() as i32, encode_postings_internal(&postings))
                    };
                    (term, df, encoded)
                })
                .collect()
        })
        .await
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
    };

    for batch in term_list.chunks(500) {
        let mut query = String::from("INSERT INTO idx_terms (term, df, postings) VALUES ");
        let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            Vec::with_capacity(batch.len() * 3);

        for (i, (term, df, postings)) in batch.iter().enumerate() {
            if i > 0 {
                query.push(',');
            }
            query.push_str(&format!("(${}, ${}, ${})", i * 3 + 1, i * 3 + 2, i * 3 + 3));
            params.push(term);
            params.push(df);
            params.push(postings);
        }

        if !merge {
            query.push_str(" ON CONFLICT DO NOTHING");
        } else {
            query.push_str(
                " ON CONFLICT (term) DO UPDATE SET df = EXCLUDED.df, postings = EXCLUDED.postings",
            );
        }

        client
            .execute(&query, &params)
            .await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    }
    Ok(())
}

async fn get_existing_terms(
    client: &tokio_postgres::Transaction<'_>,
    terms: &[&String],
) -> PyResult<FxHashMap<String, (i32, Vec<u8>)>> {
    if terms.is_empty() {
        return Ok(FxHashMap::default());
    }

    let term_strs: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
    let rows = client
        .query(
            "SELECT term, df, postings FROM idx_terms WHERE term = ANY($1)",
            &[&term_strs],
        )
        .await
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

    let mut result = FxHashMap::default();
    for row in rows {
        let term: String = row.get(0);
        let df: i32 = row.get(1);
        let postings: Vec<u8> = row.get(2);
        result.insert(term, (df, postings));
    }
    Ok(result)
}

async fn mark_books_indexed(
    client: &tokio_postgres::Transaction<'_>,
    books: &[(String, String, i32)],
) -> PyResult<()> {
    for batch in books.chunks(1000) {
        let mut query =
            String::from("INSERT INTO idx_books_indexed (book_id, file_hash, chunk_count) VALUES ");
        let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            Vec::with_capacity(batch.len() * 3);

        for (i, (book_id, hash, count)) in batch.iter().enumerate() {
            if i > 0 {
                query.push(',');
            }
            query.push_str(&format!("(${}, ${}, ${})", i * 3 + 1, i * 3 + 2, i * 3 + 3));
            params.push(book_id);
            params.push(hash);
            params.push(count);
        }

        query.push_str(" ON CONFLICT (book_id) DO UPDATE SET file_hash = EXCLUDED.file_hash, chunk_count = EXCLUDED.chunk_count");
        client
            .execute(&query, &params)
            .await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    }
    Ok(())
}

async fn get_global(client: &tokio_postgres::Client, key: &str) -> PyResult<Option<f64>> {
    let result = client
        .query_opt("SELECT value FROM idx_globals WHERE key = $1", &[&key])
        .await
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

    if let Some(row) = result {
        let val: String = row.get(0);
        Ok(val.parse().ok())
    } else {
        Ok(None)
    }
}

async fn set_global(client: &tokio_postgres::Client, key: &str, value: f64) -> PyResult<()> {
    client.execute(
        "INSERT INTO idx_globals (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        &[&key, &value.to_string()]
    ).await.map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    Ok(())
}
