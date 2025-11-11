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

1. [Install `uv`](https://docs.astral.sh/uv/getting-started/installation/) once (native packages on macOS/Linux/Windows are available).
2. Inside the repo run `uv sync`, then in two terminals run `uv run uvicorn src.api.main:app --reload` (API) and optionally `uv run streamlit run app.py`; visit `http://127.0.0.1:8000/docs` or the Streamlit URL printed in the console to try it out.

`uv sync` reuses the lockfile, so the environment is fully reproducible—no `pip install` required.

---

## 🌐 Data Sources

Boogle integrates with multiple free and public-domain repositories, such as:
- [Project Gutenberg](https://www.gutenberg.org/)
- [Open Library](https://openlibrary.org/)
- [Wikisource](https://wikisource.org/)
- [Public Domain Library](https://publicdomainlibrary.org/)
- [Internet Archive](https://archive.org/details/texts)
- [Domínio Público (Brazil)](http://www.dominiopublico.gov.br/)

---


Contact: **[enrellsa10@proton.me](mailto:enrellsa10@proton.me)**

---

## 🪪 License

This project is open-source under the **MIT License**.
Feel free to fork, modify, and improve!

---

> *Boogle — Free Books. Free Knowledge.*
