CREATE TABLE IF NOT EXISTS livros (
    id TEXT PRIMARY KEY,
    title TEXT,
    author TEXT,
    downloads INTEGER,
    cover TEXT,
    language TEXT,
    reading_level_score REAL,
    reading_level_text TEXT,
    subjects TEXT,
    summary TEXT,
    url TEXT
);
