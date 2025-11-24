# 📚 Boogle — Open Source Search Engine for Free Books

**Boogle** is an open-source search engine designed to index and search books from multiple free and public-domain sources.  
The goal is to make it easy for readers, students, and researchers to find *free and legal* books across the web —  
without having to visit each site individually.

---

## Overview

Most public-domain book collections (like Project Gutenberg or Open Library) provide their own search features,
but none of them aggregate multiple sources or offer relevance ranking based on modern information retrieval techniques.

**Boogle** changes that.
It unifies data from different repositories, builds its own index,
and returns ranked results according to query relevance — just like a miniature, open-source version of Google Books.

---

## 🚀 Quick Start (uv)

1. Start PostgreSQL with `docker compose up -d db` (default connection is `postgresql://boogle:boogle@localhost:5432/boogle`).
2. Seed metadata with `docker compose run --rm seed` (use `SEED_SOURCE=gutenberg` and `SEED_LIMIT=500` to control what runs).
3. Run the API via Docker with `docker compose up api` or locally by [installing `uv`](https://docs.astral.sh/uv/getting-started/installation/) and running `uv sync`, then `uv run uvicorn src.api.main:app --reload`.
4. Optionally start the Streamlit UI with `uv run streamlit run app.py`; visit `http://127.0.0.1:8000/docs` or the Streamlit URL printed in the console to try it out.

Set `DATABASE_URL` if you use different database credentials or a non-local host.
Use the per-source API shape `GET /metadata/{source}/{book_id}`; search responses include a `source` field so users can pick where a result comes from.

---

## 🌐 Data Sources

Boogle integrates with multiple free and public-domain repositories, such as:
- [x] [Project Gutenberg](https://www.gutenberg.org/)
- [ ] [Open Library](https://openlibrary.org/)
- [ ] [Wikisource](https://wikisource.org/)
- [ ] [Public Domain Library](https://publicdomainlibrary.org/)
- [ ] [Internet Archive](https://archive.org/details/texts)
- [ ] [Domínio Público (Brazil)](http://www.dominiopublico.gov.br/)

Contact: **[enrellsa10@proton.me](mailto:enrellsa10@proton.me)**

---

## 🪪 License

This project is open-source under the **MIT License**.
Feel free to fork, modify, and improve!

---

> *Boogle — Free Books. Free Knowledge.*
