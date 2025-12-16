import logging
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from threading import Lock
from typing import Dict, Mapping, Optional

from src.db import PostgresRepository
from src.sources.types import SourceClient


logger = logging.getLogger(__name__)


class RateLimiter:
    def __init__(self, interval_seconds: float):
        self.interval = max(0.0, interval_seconds)
        self._lock = Lock()
        self._next_time = 0.0

    def wait(self) -> None:
        if self.interval == 0:
            return
        with self._lock:
            now = time.perf_counter()
            delay = 0.0
            if now < self._next_time:
                delay = self._next_time - now
                self._next_time += self.interval
            else:
                self._next_time = now + self.interval
        if delay > 0:
            time.sleep(delay)


class SeedService:
    def __init__(
        self,
        repository: PostgresRepository,
        sources: Mapping[str, SourceClient],
        workers: int = 3,
        rate_seconds: float = 0.2,
    ):
        self.repository = repository
        self.sources = sources
        self.workers = max(1, workers)
        self.rate_limiter = RateLimiter(rate_seconds)

    def seed(self, source: Optional[str] = None, limit: Optional[int] = None) -> None:
        targets = [source] if source else list(self.sources.keys())
        logger.info(
            "Starting seed run for sources=%s limit=%s", targets, limit if limit is not None else "unbounded"
        )
        for name in targets:
            self._seed_source(name, limit)
        logger.info("Seed run completed")

    def _seed_source(self, name: str, limit: Optional[int]) -> None:
        client = self.sources.get(name)
        if not client:
            logger.error("Unsupported source requested source=%s", name)
            raise ValueError(f"Unsupported source {name}")
        offset_position, offset_book_id = self.repository.get_seed_offset(name)
        logger.info(
            "Seeding source=%s limit=%s workers=%s rate=%.3fs resume_from_position=%s last_book_id=%s",
            name,
            limit if limit is not None else "unbounded",
            self.workers,
            self.rate_limiter.interval,
            offset_position,
            offset_book_id,
        )

        count = 0
        errors = 0
        start = time.perf_counter()
        futures_position: Dict[object, int] = {}
        id_by_position: Dict[int, str] = {}
        completed_positions = set()
        next_commit = offset_position + 1
        submitted = 0
        max_in_flight = max(1, self.workers)
        in_flight = set()

        def _process(book_id: str) -> bool:
            try:
                metadata = client.extract_metadata(book_id)
                self.repository.upsert_book(metadata)
                return True
            except Exception:
                logger.exception("Failed to seed book source=%s book_id=%s", name, book_id)
                return False

        def _drain_done(done_set) -> None:
            nonlocal count, errors, next_commit
            for future in done_set:
                position = futures_position.pop(future)
                book_id = id_by_position[position]
                try:
                    ok = future.result()
                except Exception:
                    ok = False
                if ok:
                    count += 1
                else:
                    errors += 1
                completed_positions.add(position)
                while next_commit in completed_positions:
                    last_id = id_by_position.pop(next_commit, None)
                    self.repository.update_seed_offset(name, next_commit, last_id)
                    next_commit += 1
                processed = count + errors
                if processed % 500 == 0:
                    logger.info(
                        "Progress source=%s processed=%s stored=%s errors=%s checkpoint=%s",
                        name,
                        processed,
                        count,
                        errors,
                        next_commit - 1,
                    )

        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            for position, book_id in enumerate(client.iter_book_ids(limit)):
                if position <= offset_position:
                    continue
                self.rate_limiter.wait()
                future = executor.submit(_process, book_id)
                futures_position[future] = position
                id_by_position[position] = book_id
                in_flight.add(future)
                submitted += 1

                if len(in_flight) >= max_in_flight:
                    done, in_flight = wait(in_flight, return_when=FIRST_COMPLETED)
                    _drain_done(done)

            if in_flight:
                for future in as_completed(in_flight):
                    _drain_done({future})

        duration = time.perf_counter() - start
        logger.info(
            "Finished source=%s stored=%s error_count=%s duration=%.2fs submitted=%s last_committed_position=%s",
            name,
            count,
            errors,
            duration,
            submitted,
            next_commit - 1 if submitted else offset_position,
        )
