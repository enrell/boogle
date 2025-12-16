import logging
import os

from src.db import PostgresRepository
from src.seed.service import SeedService
from src.sources import get_sources


def main():
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, log_level, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    def _int_env(name: str, default: int) -> int:
        value = os.getenv(name)
        if value is None:
            return default
        try:
            return int(value)
        except ValueError:
            return default

    def _float_env(name: str, default: float) -> float:
        value = os.getenv(name)
        if value is None:
            return default
        try:
            return float(value)
        except ValueError:
            return default

    source = os.getenv("SEED_SOURCE")
    limit_value = os.getenv("SEED_LIMIT")
    limit = int(limit_value) if limit_value else None
    workers = _int_env("SEED_WORKERS", 3)
    rate_seconds = _float_env("SEED_RATE_SECONDS", 0.2)
    sources = get_sources()
    service = SeedService(PostgresRepository(), sources, workers=workers, rate_seconds=rate_seconds)
    service.seed(source.lower() if source else None, limit)


if __name__ == "__main__":
    main()
