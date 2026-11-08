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

## 🚀 Quick Start (Frontend)

This is the frontend repository for Boogle, built with [React Router](https://reactrouter.com/).

### Prerequisites

- Node.js (v20 or later recommended)
- Bun (v1.0 or later) easy install: [Bun Official Site](https://bun.com/)

### Installation

1. Clone the Boogle repository (the frontend lives in `frontend/`):
   ```bash
   git clone https://github.com/enrell/boogle.git
   cd boogle/frontend
   ```

2. Install dependencies:
   ```bash
   bun install
   ```

### Development

Run the development server:

```bash
bun run dev
```

Open your browser and visit `http://localhost:5173` (or the URL shown in the console).

### Production

Build the application for production:

```bash
bun run build
```

Start the production server:

```bash
bun start
```

---

## 🌐 Data Sources

Boogle integrates with multiple free and public-domain repositories, such as:
- [x] [Project Gutenberg](https://www.gutenberg.org/)
- [ ] [Open Library](https://openlibrary.org/)
- [ ] [Wikisource](https://wikisource.org/)
- [ ] [Public Domain Library](https://publicdomainlibrary.org/)
- [ ] [Internet Archive](https://archive.org/details/texts)
- [ ] [Domínio Público (Brazil)](http://www.dominiopublico.gov.br/)

---

Contact: **[enrellsa10@proton.me](mailto:enrellsa10@proton.me)**

---

## 🪪 License

This project is open-source under the **MIT License**.
Feel free to fork, modify, and improve!

---

> *Boogle — Free Books. Free Knowledge.*
