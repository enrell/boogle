#!/usr/bin/env python3
import argparse
import os
import sys

import psycopg


def get_dsn() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    user = os.getenv("POSTGRES_USER", "boogle")
    password = os.getenv("POSTGRES_PASSWORD", "boogle")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "boogle")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def migrate():
    with psycopg.connect(get_dsn()) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS books (
                id BIGSERIAL PRIMARY KEY,
                source TEXT NOT NULL,
                book_id TEXT NOT NULL,
                url TEXT,
                title TEXT,
                author TEXT,
                illustrator TEXT,
                release_date TEXT,
                language TEXT,
                category TEXT,
                original_publication TEXT,
                credits TEXT,
                copyright_status TEXT,
                downloads TEXT,
                files JSONB NOT NULL DEFAULT '[]'::jsonb,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(source, book_id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS idx_documents (
                doc_id INTEGER PRIMARY KEY,
                length INTEGER NOT NULL,
                metadata TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS idx_terms (
                term TEXT PRIMARY KEY,
                df INTEGER NOT NULL,
                postings BYTEA NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS idx_globals (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        conn.commit()
    print("Migration complete")


def test():
    try:
        with psycopg.connect(get_dsn()) as conn:
            row = conn.execute("SELECT 1 as ok").fetchone()
            assert row[0] == 1
            
            tables = conn.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name LIKE 'idx_%'
            """).fetchall()
            
            print(f"Connection: OK")
            print(f"Tables: {[t[0] for t in tables]}")
            
            for table in ['idx_documents', 'idx_terms', 'idx_globals']:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                print(f"  {table}: {count} rows")
            
            globals_data = conn.execute("SELECT key, value FROM idx_globals").fetchall()
            if globals_data:
                print("Globals:")
                for key, value in globals_data:
                    print(f"  {key}: {value}")
                    
    except Exception as e:
        print(f"Connection FAILED: {e}")
        sys.exit(1)


def clear():
    with psycopg.connect(get_dsn()) as conn:
        conn.execute("TRUNCATE idx_documents, idx_terms, idx_globals")
        conn.commit()
    print("Index data cleared")


def clear_all():
    with psycopg.connect(get_dsn()) as conn:
        conn.execute("TRUNCATE books, idx_documents, idx_terms, idx_globals")
        conn.commit()
    print("All data cleared")


def drop():
    with psycopg.connect(get_dsn()) as conn:
        conn.execute("DROP TABLE IF EXISTS books, idx_documents, idx_terms, idx_globals CASCADE")
        conn.commit()
    print("All tables dropped")


def main():
    parser = argparse.ArgumentParser(description="Database management")
    parser.add_argument("command", choices=["migrate", "test", "clear", "clear-all", "drop"])
    args = parser.parse_args()
    
    commands = {
        "migrate": migrate,
        "test": test,
        "clear": clear,
        "clear-all": clear_all,
        "drop": drop,
    }
    commands[args.command]()


if __name__ == "__main__":
    main()
