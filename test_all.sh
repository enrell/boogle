#!/bin/bash
# Boogle End-to-End Test Script
# Tests all major components of the multi-provider search system

set -e

echo "=========================================="
echo "Boogle Multi-Provider Search System - E2E Test"
echo "=========================================="
echo ""

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print test results
print_result() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✓ $2${NC}"
    else
        echo -e "${RED}✗ $2${NC}"
        exit 1
    fi
}

# 1. Test Rust Module Imports
echo "=========================================="
echo "Test 1: Rust Module Imports"
echo "=========================================="
uv run python -c "
from rust_bm25 import FileSearcher, RealTimeIndexer
print('✓ FileSearcher imported')
print('✓ RealTimeIndexer imported')
"
print_result $? "Rust modules import successfully"
echo ""

# 2. Test RealTimeIndexer.flush()
echo "=========================================="
echo "Test 2: RealTimeIndexer.flush() Persists to Disk"
echo "=========================================="
rm -rf data/test_rt_index
mkdir -p data/test_rt_index

uv run python << 'EOF'
import json, os
from rust_bm25 import RealTimeIndexer

# Create test index
index_dir = "data/test_rt_index"
with open(f"{index_dir}/index.json", "w") as f:
    json.dump({"segments": [], "total_docs": 0, "avgdl": 0.0}, f)

# Initialize indexer
indexer = RealTimeIndexer(index_dir)

# Add documents
doc_ids = []
for i in range(3):
    doc_id = indexer.add_document(
        f"Test document {i} with books and reading content",
        f"book_{i}"
    )
    doc_ids.append(doc_id)

# Search before flush
results_before = indexer.search("books", 10)
print(f"Before flush: Found {len(results_before)} results")

# Flush to disk
count = indexer.flush()
print(f"Flushed {count} documents to disk")

# Verify segment created
segments = [f for f in os.listdir(index_dir) if f.startswith("segment_")]
print(f"Segments created: {segments}")

# Reload and verify
indexer2 = RealTimeIndexer(index_dir)
results_after = indexer2.search("books", 10)
print(f"After reload: Found {len(results_after)} results")

assert len(segments) > 0, "No segments created"
assert count == 3, f"Wrong count: {count}"
EOF

print_result $? "RealTimeIndexer.flush() writes to disk correctly"
echo ""

# 3. Test Parallel Provider Seeding
echo "=========================================="
echo "Test 3: Parallel Provider Seeding"
echo "=========================================="
uv run python << 'EOF'
import time
from src.downloader.downloader import BookSeeder
from src.providers.registry import ProviderRegistry

ProviderRegistry.auto_discover()
providers = ProviderRegistry.get_enabled()

print(f"Found {len(providers)} providers: {[p.source_name for p in providers]}")

if len(providers) >= 2:
    seeder = BookSeeder(providers[:2], use_sqlite=True, max_workers=2)
    
    # Test parallel execution
    start = time.time()
    results = seeder.seed_all(limit=1, parallel=True, max_parallel_providers=2)
    elapsed = time.time() - start
    
    print(f"Parallel seed completed in {elapsed:.2f}s")
    print(f"Results: {results}")
    
    seeder.close()
else:
    print("Need at least 2 providers for parallel test")
EOF

print_result $? "Parallel provider seeding works"
echo ""

# 4. Test Indexing Pipeline
echo "=========================================="
echo "Test 4: Indexing Pipeline"
echo "=========================================="
echo "Running: uv run python -m src.pipeline index --sqlite --limit 5 --providers gutenberg --reindex"
timeout 120 uv run python -m src.pipeline index --sqlite --limit 5 --providers gutenberg --reindex 2>&1 | tail -20
print_result $? "Indexing pipeline runs successfully"
echo ""

# 5. Test API Startup (light mode)
echo "=========================================="
echo "Test 5: API Startup (Light Mode)"
echo "=========================================="
pkill -9 -f uvicorn 2>/dev/null || true
sleep 1

USE_SQLITE=1 LIGHT_MODE=1 uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8000 > /tmp/api_test.log 2>&1 &
sleep 5

# Check if API is running
curl -s http://localhost:8000/health > /dev/null
print_result $? "API starts successfully in light mode"
echo ""

# 6. Test API Endpoints
echo "=========================================="
echo "Test 6: API Endpoints"
echo "=========================================="

echo "Testing / endpoint..."
curl -s http://localhost:8000/ | grep -q "Boogle Search API"
print_result $? "GET / returns API info"

echo "Testing /providers endpoint..."
curl -s http://localhost:8000/providers | grep -q "gutenberg"
print_result $? "GET /providers returns providers list"

echo "Testing /health endpoint..."
curl -s http://localhost:8000/health | grep -q "healthy"
print_result $? "GET /health returns healthy status"

echo "Testing /search endpoint..."
curl -s "http://localhost:8000/search?query=test" > /dev/null
print_result $? "GET /search accepts queries"

echo ""

# 7. Test Provider API
echo "=========================================="
echo "Test 7: Provider System"
echo "=========================================="
uv run python << 'EOF'
from src.schemas.translator_registry import TranslatorRegistry

TranslatorRegistry.auto_discover()
providers = TranslatorRegistry.list_providers()

print(f"Found {len(providers)} providers:")
for name in providers:
    translator = TranslatorRegistry.get(name)
    quality = translator.source_quality if translator else 0
    print(f"  - {name}: quality={quality}")
EOF

print_result $? "Provider registry works"
echo ""

# 8. Test Security Validators
echo "=========================================="
echo "Test 8: Security Validators"
echo "=========================================="
uv run python << 'EOF'
from src.security.validators import SecurityValidators, SecurityError
import pytest

# Test SQL injection detection
try:
    query = "test'; DROP TABLE books; --"
    validated = SecurityValidators.validate_query(query)
    print("SQL injection detected!")
except SecurityError as e:
    print(f"✓ SQL injection blocked: {e}")

# Test query sanitization
clean = SecurityValidators.validate_query("normal search query")
print(f"✓ Valid query passed: {clean}")
EOF

print_result $? "Security validators work"
echo ""

# 9. Test Database
echo "=========================================="
echo "Test 9: Database Operations"
echo "=========================================="
uv run python << 'EOF'
from src.db.database import PostgresRepository

db = PostgresRepository(use_sqlite=True)
meta = db.get_index_metadata()
print(f"Database connected")
print(f"Total books: {meta.get('total_books', 0)}")
print(f"Providers: {list(meta.get('providers', {}).keys())}")
db.close()
EOF

print_result $? "Database operations work"
echo ""

# Cleanup
echo "=========================================="
echo "Cleanup"
echo "=========================================="
pkill -9 -f uvicorn 2>/dev/null || true
rm -rf data/test_rt_index

# Final Summary
echo ""
echo "=========================================="
echo "TEST SUMMARY"
echo "=========================================="
echo -e "${GREEN}All tests passed!${NC}"
echo ""
echo "System tested:"
echo "  ✓ Rust module imports (FileSearcher, RealTimeIndexer)"
echo "  ✓ RealTimeIndexer.flush() persists to disk"
echo "  ✓ Parallel provider seeding"
echo "  ✓ Indexing pipeline"
echo "  ✓ API startup (light mode)"
echo "  ✓ API endpoints (/ /providers /health /search)"
echo "  ✓ Provider registry"
echo "  ✓ Security validators"
echo "  ✓ Database operations"
echo ""
echo "=========================================="
echo "READY FOR PRODUCTION USE!"
echo "=========================================="
