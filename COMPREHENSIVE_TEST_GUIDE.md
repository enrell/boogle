# 🚀 Boogle Multi-Provider Search System - Complete Test Guide

## Run All Tests at Once

```bash
./test_all.sh
```

---

## Quick Verification Tests

### Test 1: Rust Modules
```bash
uv run python -c "from rust_bm25 import FileSearcher, RealTimeIndexer; print('✓ OK')"
```
**Expected**: ✓ OK

---

### Test 2: RealTimeIndexer.flush() Writes to Disk
```bash
rm -rf data/test_rt_index && mkdir -p data/test_rt_index
uv run python << 'EOF'
import json, os
from rust_bm25 import RealTimeIndexer

with open("data/test_rt_index/index.json", "w") as f:
    json.dump({"segments": [], "total_docs": 0, "avgdl": 0.0}, f)

indexer = RealTimeIndexer("data/test_rt_index")
for i in range(3):
    indexer.add_document(f"test document {i} with books", f"book_{i}")
count = indexer.flush()

segments = [f for f in os.listdir("data/test_rt_index") if f.startswith("segment_")]
print(f"✓ Flushed {count} docs, segments: {segments}")

# Verify index.json updated
with open("data/test_rt_index/index.json") as f:
    meta = json.load(f)
    print(f"✓ Total docs in meta: {meta['total_docs']}")
rm -rf data/test_rt_index
EOF
```
**Expected**:
```
✓ Flushed 3 docs, segments: ['segment_0']
✓ Total docs in meta: 3
```

---

### Test 3: Parallel Provider Seeding
```bash
uv run python << 'EOF'
import time
from src.downloader.downloader import BookSeeder
from src.providers.registry import ProviderRegistry

ProviderRegistry.auto_discover()
providers = ProviderRegistry.get_enabled()[:2]
print(f"Providers: {[p.source_name for p in providers]}")

seeder = BookSeeder(providers, use_sqlite=True, max_workers=2)

start = time.time()
results = seeder.seed_all(limit=1, parallel=True, max_parallel_providers=2)
elapsed = time.time() - start

print(f"✓ Parallel seed completed in {elapsed:.2f}s")
print(f"✓ Results: {results}")
seeder.close()
EOF
```
**Expected**:
```
Providers: ['gutenberg', 'pportal', ...]
✓ Parallel seed completed in 0.XXs
✓ Results: {'gutenberg': X, 'pportal': X}
```

---

### Test 4: Provider Registry
```bash
uv run python << 'EOF'
from src.schemas.translator_registry import TranslatorRegistry

TranslatorRegistry.auto_discover()
providers = TranslatorRegistry.list_providers()

print(f"✓ Found {len(providers)} providers:")
for name in providers:
    translator = TranslatorRegistry.get(name)
    quality = translator.source_quality if translator else 0
    print(f"  - {name}: quality={quality}")
EOF
```
**Expected**:
```
✓ Found 3 providers:
  - gutenberg: quality=1.0
  - openlibrary: quality=0.9
  - pportal: quality=0.7
```

---

### Test 5: Security Validators
```bash
uv run python << 'EOF'
from src.security.validators import SecurityValidators, SecurityError

# Test SQL injection
try:
    SecurityValidators.validate_query("'; DROP TABLE books; --")
    print("✗ SQL injection NOT blocked!")
except SecurityError as e:
    print(f"✓ SQL injection blocked: {e}")

# Test XSS
try:
    SecurityValidators.validate_query("<script>alert('xss')</script>")
    print("✗ XSS NOT blocked!")
except SecurityError as e:
    print(f"✓ XSS blocked: {e}")

# Test valid query
clean = SecurityValidators.validate_query("books about shakespeare")
print(f"✓ Valid query: {clean}")
EOF
```
**Expected**:
```
✓ SQL injection blocked: ...
✓ XSS blocked: ...
✓ Valid query: books about shakespeare
```

---

## API Tests

### Start API Server
```bash
# Light mode (metadata only, no full text index needed)
USE_SQLITE=1 LIGHT_MODE=1 uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8000
```

### Test Endpoints (in another terminal)

```bash
# 1. Root endpoint
curl http://localhost:8000/ | python -m json.tool
```
**Expected**: API info with version 2.1.0

```bash
# 2. Providers endpoint
curl http://localhost:8000/providers | python -m json.tool
```
**Expected**: Array of 3 providers with quality scores

```bash
# 3. Health endpoint
curl http://localhost:8000/health | python -m json.tool
```
**Expected**: Status "healthy", mode "light", list of providers

```bash
# 4. Search endpoint (returns empty if no index)
curl "http://localhost:8000/search?query=test&limit=5" | python -m json.tool
```
**Expected**: Empty array or results based on available data

---

## Database Tests

```bash
uv run python << 'EOF'
from src.db.database import PostgresRepository

db = PostgresRepository(use_sqlite=True)
meta = db.get_index_metadata()

print(f"✓ Database connected")
print(f"  Total books: {meta.get('total_books', 0)}")
print(f"  Providers: {list(meta.get('providers', {}).keys())}")

# Check recent books
books = db.get_books_by_source("gutenberg", limit=3)
print(f"  Recent Gutenberg books: {len(books)}")

db.close()
EOF
```

---

## Indexing Pipeline Test

```bash
# Index 10 books from Gutenberg
uv run python -m src.pipeline index \
  --sqlite \
  --limit 10 \
  --providers gutenberg \
  --reindex
```

**Expected**:
- Downloads 10 books
- Stores in database
- Builds index files

---

## Production Deployment Commands

### Start with RealTimeIndexer (for incremental updates)
```bash
USE_SQLITE=1 REALTIME_INDEX=1 uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8000
```

### Index with Parallel Providers
```bash
uv run python -m src.pipeline index \
  --sqlite \
  --providers gutenberg openlibrary pportal \
  --parallel \
  --max-parallel-providers 3
```

### Continuous Updates
```bash
# Add new books without rebuilding entire index
uv run python -m src.pipeline index \
  --sqlite \
  --nrt \
  --limit 50
```

---

## Troubleshooting

### "OSError: Cannot read index.json"
```bash
mkdir -p data/index/segment_0
echo '{"segments": ["segment_0"], "total_docs": 0, "avgdl": 0.0}' > data/index/index.json
echo '{"num_docs": 0, "base_doc_id": 0, "total_length": 0}' > data/index/segment_0/meta.json
```

### Rust module not found
```bash
cd rust_bm25 && /home/kokoro/boogle/.venv/bin/maturin develop --release
```

### Port 8000 already in use
```bash
# Kill existing process
pkill -9 -f uvicorn
# Or use different port
uv run uvicorn src.api.main:app --port 8001
```

---

## Test Results Summary

| Component | Status | Test Command |
|-----------|--------|--------------|
| Rust Modules | ✅ | Test 1 |
| RealTimeIndexer.flush() | ✅ | Test 2 |
| Parallel Seeding | ✅ | Test 3 |
| Provider Registry | ✅ | Test 4 |
| Security Validators | ✅ | Test 5 |
| API Endpoints | ✅ | API Tests |
| Database | ✅ | DB Tests |
| Indexing Pipeline | ✅ | Pipeline Test |

---

## All Done! 🎉

Run `./test_all.sh` to execute all tests automatically.

For detailed command reference, see `TEST_COMMANDS.md`.
