import os
import shutil
import pytest
from pathlib import Path

def pytest_addoption(parser):
    parser.addoption("--sqlite", action="store_true", default=False, help="Use temporary SQLite database instead of Postgres")

@pytest.fixture(scope="session")
def test_dirs():
    """Setup temporary directories for file storage and index."""
    base = Path("tests/data")
    books_dir = base / "books"
    index_dir = base / "index"
    chunks_dir = base / "chunks"
    
    # Clean setup
    if base.exists():
        shutil.rmtree(base)
    
    # Create directories
    books_dir.mkdir(parents=True, exist_ok=True)
    index_dir.mkdir(parents=True, exist_ok=True)
    chunks_dir.mkdir(parents=True, exist_ok=True)
    
    yield {
        "base": str(base),
        "books": str(books_dir),
        "index": str(index_dir),
        "chunks": str(chunks_dir)
    }
    
    # Teardown
    if base.exists():
        shutil.rmtree(base)

@pytest.fixture(scope="session")
def test_db_env(test_dirs, request):
    """Override environment variables to use test database and directories."""
    use_sqlite = request.config.getoption("--sqlite")
    
    # Store original env vars
    original_env = {
        "USE_SQLITE": os.environ.get("USE_SQLITE"),
        "SQLITE_DB_PATH": os.environ.get("SQLITE_DB_PATH"),
        "INDEX_DIR": os.environ.get("INDEX_DIR"),
        "CHUNKS_DIR": os.environ.get("CHUNKS_DIR"),
    }
    
    # Set test env vars
    os.environ["BOOKS_DIR"] = test_dirs["books"]
    os.environ["INDEX_DIR"] = test_dirs["index"]
    os.environ["CHUNKS_DIR"] = test_dirs["chunks"]
    
    if use_sqlite:
        db_path = os.path.join(test_dirs["base"], "test.db")
        os.environ["USE_SQLITE"] = "1"
        os.environ["SQLITE_DB_PATH"] = db_path
    else:
        # Assume Postgres is available (e.g. from Docker)
        # We might want to set a test specific DB name if possible, 
        # but for now we trust the default env or docker compose Setup.
        if "USE_SQLITE" in os.environ:
             del os.environ["USE_SQLITE"]
    
    yield
    
    # Restore original env vars
    for key, value in original_env.items():
        if value is None:
            if key in os.environ:
                del os.environ[key]
        else:
            os.environ[key] = value
