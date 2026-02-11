#!/bin/bash
set -e

echo "=========================================="
echo "Boogle Multi-Provider System Test"
echo "=========================================="
echo ""

# Clean data
echo "1. Cleaning data folder..."
rm -rf data/
mkdir -p data
echo "   ✓ Data folder cleaned"
echo ""

# Test 1: Index all providers with limit
echo "2. Indexing all enabled providers (limit=5)..."
uv run python -m src.pipeline index --sqlite --limit 5 --light-mode 2>&1 | grep -E "(Seeding complete|Results:|Metadata indexing)"
echo ""

# Test 2: Verify database
echo "3. Verifying database..."
uv run python -c "
from src.db.database import PostgresRepository
db = PostgresRepository(use_sqlite=True)
for source in ['gutenberg', 'pportal']:
    books = db.get_books_by_source(source)
    print(f'   ✓ {source}: {len(books)} books')
"
echo ""

# Test 3: Incremental seeding (should find 0)
echo "4. Testing incremental seeding (should find 0 new)..."
uv run python -m src.pipeline index --sqlite --limit 5 --light-mode 2>&1 | grep -E "(Seeding complete:|No new books)"
echo ""

# Test 4: Check checkpoints
echo "5. Checking checkpoints..."
uv run python -c "
from src.db.database import PostgresRepository
db = PostgresRepository(use_sqlite=True)
for source in ['gutenberg', 'pportal']:
    pos, last = db.get_seed_offset(source)
    print(f'   ✓ {source}: position={pos}, last_book={last}')
"
echo ""

echo "=========================================="
echo "All tests passed!"
echo "=========================================="
