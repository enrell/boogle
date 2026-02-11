"""
Translator Registry

Manages translator instances and provides lookup by provider name.
"""

from typing import Dict, Type, Optional
from src.schemas.translator import SchemaTranslator
from src.translators.gutenberg_translator import GutenbergTranslator
from src.translators.openlibrary_translator import OpenLibraryTranslator
from src.translators.pportal_translator import PPORTALTranslator


class TranslatorRegistry:
    """
    Registry for SchemaTranslator instances.

    Usage:
        translator = TranslatorRegistry.get('gutenberg')
        unified = translator.translate(raw_metadata)
    """

    _translators: Dict[str, SchemaTranslator] = {}
    _translator_classes: Dict[str, Type[SchemaTranslator]] = {
        "gutenberg": GutenbergTranslator,
        "openlibrary": OpenLibraryTranslator,
        "pportal": PPORTALTranslator,
    }

    @classmethod
    def register(cls, provider: str, translator_class: Type[SchemaTranslator]) -> None:
        """Register a translator class for a provider."""
        cls._translator_classes[provider] = translator_class
        # Clear cached instance if exists
        if provider in cls._translators:
            del cls._translators[provider]

    @classmethod
    def get(cls, provider: str) -> Optional[SchemaTranslator]:
        """Get translator instance for a provider."""
        if provider not in cls._translators:
            if provider not in cls._translator_classes:
                return None
            # Create instance
            translator_class = cls._translator_classes[provider]
            cls._translators[provider] = translator_class()

        return cls._translators[provider]

    @classmethod
    def get_fallback(cls) -> SchemaTranslator:
        """Get fallback translator for unknown providers."""
        from src.schemas.translator import FallbackTranslator

        return FallbackTranslator()

    @classmethod
    def translate(cls, provider: str, raw: Dict) -> Optional[Dict]:
        """
        Convenience method to translate metadata.

        Returns unified metadata dict or None if provider not found.
        """
        translator = cls.get(provider)
        if translator:
            return translator.translate(raw).to_dict()

        # Use fallback
        fallback = cls.get_fallback()
        ctx = type("Context", (), {"provider_name": provider, "raw_metadata": raw})()
        return fallback.translate(raw).to_dict()

    @classmethod
    def list_providers(cls) -> list:
        """List all registered providers."""
        cls._auto_discover()
        return list(cls._translator_classes.keys())

    @classmethod
    def _auto_discover(cls) -> None:
        """Auto-discover translators from translators package."""
        import importlib
        import pkgutil
        from src.schemas.translator import SchemaTranslator

        try:
            import src.translators as translators_pkg

            for importer, modname, ispkg in pkgutil.iter_modules(
                translators_pkg.__path__, translators_pkg.__name__ + "."
            ):
                if modname.endswith(("_translator",)):
                    try:
                        importlib.import_module(modname)
                    except Exception as e:
                        print(
                            f"Warning: Failed to load translator module {modname}: {e}"
                        )
        except ImportError:
            pass

    @classmethod
    def auto_discover(cls) -> None:
        """Public method to trigger auto-discovery."""
        cls._auto_discover()

    @classmethod
    def get_source_quality(cls, provider: str) -> float:
        """Get quality score for a provider."""
        translator = cls.get(provider)
        if translator:
            return translator.source_quality
        return 0.5  # Default
