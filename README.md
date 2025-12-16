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

## 🚀 Quick Start

```bash
docker compose up -d db

uv run boogle seed --limit 1000

uv run boogle index

uv run uvicorn src.api.main:app --reload
```

Visit `http://127.0.0.1:8000/docs` to try the API.

### Commands

| Command | Description |
|---------|-------------|
| `boogle seed` | Download books from Gutenberg |
| `boogle seed --refresh` | Update metadata for existing books |
| `boogle index` | Build/update search index |
| `boogle search "query"` | Search from CLI |

---

## 🌐 Data Sources

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
