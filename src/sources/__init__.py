from typing import Mapping

from src.scraper.scraper import GutenbergScraper
from src.sources.types import SourceClient

sources: Mapping[str, SourceClient] = {"gutenberg": GutenbergScraper()}


def get_sources() -> Mapping[str, SourceClient]:
    return sources
