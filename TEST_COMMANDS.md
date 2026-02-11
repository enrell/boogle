# Boogle Multi-Provider Search System - Test Commands

## Quick Start (All-in-one test)
```bash
bash test_all.sh
```

## Individual Test Commands

### 1. Test Rust Module Imports
```bash
uv run python -c "from rust_bm25 import FileSearcher, RealTimeIndexer; print('✓ Rust modules imported')"
```

### 2. Test RealTimeIndexer.flush()
```bash
rm -rf data/test_rt_index
mkdir -p data/test_rt_index
uv run python << 'EOF'
import json, os
from rust_bm25 import RealTimeIndexer

with open("data/test_rt_index/index.json", "w") as f:
    json.dump({"segments": [], "total_docs": 0, "avgdl": 0.0}, f)

indexer = RealTimeIndexer("data/test_rt_index")
for i in range(3):
    indexer.add_document(f"Test doc {i} with books content", f"book_{i}")
count = indexer.flush()
segments = [f for f in os.listdir("data/test_rt_index") if f.startswith("segment_")]
print(f"✓ Flushed {count} docs, segments: {segments}")
EOF
```

### 3. Test Parallel Provider Seeding
```bash
uv run python << 'EOF'
import time
from src.downloader.downloader import BookSeeder
from src.providers.registry import ProviderRegistry

ProviderRegistry.auto_discover()
providers = ProviderRegistry.get_enabled()[:2]
seeder = BookSeeder(providers, use_sqlite=True, max_workers=2)

start = time.time()
results = seeder.seed_all(limit=1, parallel=True, max_parallel_providers=2)
elapsed = time.time() - start

print(f"✓ Parallel seed: {elapsed:.2f}s, results: {results}")
seeder.close()
EOF
```

### 4. Run Indexing Pipeline
```bash
# Index 10 books from Gutenberg
uv run python -m src.pipeline index --sqlite --limit 10 --providers gutenberg --reindex
```

### 5. Start API Server (Light Mode)
```bash
USE_SQLITE=1 LIGHT_MODE=1 uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8000
```

### 6. Test API Endpoints (in another terminal)
```bash
# Root
curl http://localhost:8000/ | python -m json.tool

# Providers
curl http://localhost:8000/providers | python -m json.tool

# Health
curl http://localhost:8000/health | python -m json.tool

# Search
curl "http://localhost:8000/search?query=shakespeare&limit=5" | python -m json.tool
```

### 7. Test Provider Registry
```bash
uv run python << 'EOF'
from src.schemas.translator_registry import TranslatorRegistry

TranslatorRegistry.auto_discover()
providers = TranslatorRegistry.list_providers()

for name in providers:
    translator = TranslatorRegistry.get(name)
    quality = translator.source_quality if translator else 0
    print(f"  - {name}: quality={quality}")

print(f"\n✓ Found {len(providers)} providers")
EOF
```

### 8. Test Security Validators
```bash
uv run python << 'EOF'
from src.security.validators import SecurityValidators, SecurityError

# Test SQL injection detection
try:
    SecurityValidators.validate_query("test'; DROP TABLE books; --")
    print("SQL injection should have been blocked!")
except SecurityError as e:
    print(f"✓ SQL injection blocked: {e}")

# Test valid query
clean = SecurityValidators.validate_query("normal search query")
print(f"✓ Valid query passed: {clean}")
EOF
```

### 9. Test Database
```bash
uv run python << 'EOF'
from src.db.database import PostgresRepository

db = PostgresRepository(use_sqlite=True)
meta = db.get_index_metadata()
print(f"Total books: {meta.get('total_books', 0)}")
print(f"Providers: {list(meta.get('providers', {}).keys())}")
db.close()
EOF
```

### 10. Full End-to-End Test (Recommended)
```bash
# Run all tests sequentially
bash test_all.sh
```

## Production Deployment Commands

### Start API with RealTimeIndexer
```bash
USE_SQLITE=1 REALTIME_INDEX=1 uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8000
```

### Index with Parallel Providers
```bash
# Seed from multiple providers in parallel
uv run python -m src.pipeline index --sqlite --providers gutenberg openlibrary --parallel
```

### Continuous Update with NRT
```bash
# Use RealTimeIndexer for incremental updates
uv run python -m src.pipeline index --sqlite --nrt
```

## Cleanup Commands

```bash
# Stop API
pkill -9 -f uvicorn

# Clean test data
rm -rf data/test_rt_index

# Reset database (with caution)
rm data/boogle.db

# Regenerate everything
uv run python -m src.pipeline index --sqlite --reindex
```

## Troubleshooting

### API won't start - "OSError: Cannot read index.json"
```bash
# Create minimal index structure
mkdir -p data/index/segment_0
echo '{"segments": ["segment_0"], "total_docs": 0, "avgdl": 0.0}' > data/index/index.json
echo '{"num_docs": 0, "base_doc_id": 0, "total_length": 0}' > data/index/segment_0/meta.json
```

### Rust module not found
```bash
cd rust_bm25 && /home/kokoro/boogle/.venv/bin/maturin develop --release
```

### Database locked (SQLite)
```bash
# Stop all processes
pkill -9 -f uvicorn
pkill -9 -f python

# Retry
```
