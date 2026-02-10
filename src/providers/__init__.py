"""
Book Providers Module

This module provides a pluggable architecture for adding book providers to Boogle.
Each provider implements the BaseBookProvider interface and is automatically discovered
at runtime.

Example:
    from src.providers.base import BaseBookProvider
    from src.providers.registry import ProviderRegistry, register_provider

    @register_provider
    class MyProvider(BaseBookProvider):
        @property
        def source_name(self) -> str:
            return "myprovider"

        def iter_book_metadata(self, limit=None):
            # Yield book metadata dicts
            pass
"""

from src.providers.base import BaseBookProvider
from src.providers.registry import ProviderRegistry, register_provider

__all__ = ["BaseBookProvider", "ProviderRegistry", "register_provider"]
