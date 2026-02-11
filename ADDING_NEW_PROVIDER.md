# Adding a New Provider - Step-by-Step Guide

## Quick Summary

To add a new provider, you need to create **ONE file** (the translator). Everything else is automatic.

## Step-by-Step Instructions

### Step 1: Create Translator File

Create `src/translators/{your_provider}_translator.py`:

```python
"""
Your Provider Translator

Translate your provider's metadata to UnifiedBookMetadata.
"""

from typing import Dict, Any, List
from src.schemas.translator import SchemaTranslator, TranslationContext
from src.schemas.unified_metadata import (
    Contributor,
    FileInfo,
    ProviderSource,
    ImageInfo,
    RatingInfo,
    PopularityInfo
)


class YourProviderTranslator(SchemaTranslator):
    """Translator for Your Provider."""
    
    @property
    def provider_name(self) -> str:
        """Return your provider's name."""
        return "your_provider"  # lowercase, no spaces
    
    @property
    def source_quality(self) -> float:
        """
        Return quality score (0.0-1.0).
        
        Guidelines:
        - 1.0: Premium curated (Gutenberg)
        - 0.8-0.9: Good quality curated (OpenLibrary)
        - 0.6-0.7: Aggregated sources (PPORTAL)
        - 0.5: Unknown/low quality
        """
        return 0.8
    
    def translate_core(self, raw: Dict[str, Any], ctx: TranslationContext) -> Dict[str, Any]:
        """
        Translate core required fields.
        
        Required: canonical_id, title, language, source_url
        """
        return {
            'canonical_id': self.generate_canonical_id(raw, ctx),
            'title': raw.get('title', 'Unknown'),
            'language': self._normalize_language(raw.get('language')),
            'source_url': raw.get('url') or self._build_url(raw.get('book_id')),
        }
    
    def translate_bibliographic(self, raw: Dict[str, Any], ctx: TranslationContext) -> Dict[str, Any]:
        """Translate bibliographic fields."""
        return {
            'authors': self.normalize_author(raw.get('author')),
            'subjects': self.parse_subjects(raw.get('subjects')),
            'publication_year': self.extract_year(raw.get('date')),
            'publisher': raw.get('publisher'),
        }
    
    def translate_content(self, raw: Dict[str, Any], ctx: TranslationContext) -> Dict[str, Any]:
        """Translate content/description fields."""
        return {
            'description': raw.get('description'),
            'copyright_status': raw.get('license'),
        }
    
    def translate_media(self, raw: Dict[str, Any], ctx: TranslationContext) -> Dict[str, Any]:
        """Translate media (files, covers)."""
        files = []
        for f in raw.get('files', []):
            files.append(FileInfo(
                format=f.get('format', 'unknown'),
                url=f.get('url', ''),
                size=f.get('size')
            ))
        
        return {
            'files': files,
            'cover_image': ImageInfo(url=raw['cover_url']) if raw.get('cover_url') else None,
        }
    
    def translate_identifiers(self, raw: Dict[str, Any], ctx: TranslationContext) -> Dict[str, Any]:
        """Translate identifiers."""
        return {
            'isbn_13': raw.get('isbn_13'),
            'provider_ids': {self.provider_name: str(raw.get('book_id', ''))},
        }
    
    def translate_enrichment(self, raw: Dict[str, Any], ctx: TranslationContext) -> Dict[str, Any]:
        """Translate enrichment/popularity data."""
        popularity = None
        if raw.get('downloads'):
            popularity = PopularityInfo(downloads=raw['downloads'])
        
        return {
            'popularity': popularity,
        }
    
    # Helper methods (optional)
    def _normalize_language(self, lang: str) -> str:
        """Normalize language code."""
        if not lang:
            return 'en'
        return lang.lower()[:2]
    
    def _build_url(self, book_id: str) -> str:
        """Build canonical URL."""
        return f"https://your-provider.com/book/{book_id}"
```

### Step 2: Register the Translator

Add to `src/translators/__init__.py`:

```python
from src.translators.your_provider_translator import YourProviderTranslator

__all__ = [
    # ... existing translators ...
    "YourProviderTranslator",
]
```

### Step 3: Add to Registry (Optional - for explicit registration)

If you want explicit control, add to `src/schemas/translator_registry.py`:

```python
from src.translators.your_provider_translator import YourProviderTranslator

class TranslatorRegistry:
    _translator_classes: Dict[str, Type[SchemaTranslator]] = {
        # ... existing ...
        'your_provider': YourProviderTranslator,
    }
```

**Note**: If you don't add it here, it will be auto-discovered via the `@register_provider` decorator!

### Step 4: Test Your Translator

Create test file `tests/translators/test_your_provider.py`:

```python
import pytest
from src.translators.your_provider_translator import YourProviderTranslator


class TestYourProviderTranslator:
    @pytest.fixture
    def translator(self):
        return YourProviderTranslator()
    
    def test_translate_basic_book(self, translator):
        raw = {
            'book_id': '12345',
            'title': 'Test Book',
            'author': 'John Doe',
            'language': 'en',
        }
        
        result = translator.translate(raw)
        
        assert result.title == 'Test Book'
        assert result.authors[0].name == 'John Doe'
        assert result.primary_source.provider == 'your_provider'
```

Run tests:
```bash
uv run python -m pytest tests/translators/test_your_provider.py -v
```

### Step 5: Security Validation (Automatic!)

Security is **automatic** via the schema:

- SQL injection patterns are detected
- XSS is sanitized
- Path traversal is blocked
- Length limits are enforced
- Type validation happens via Pydantic

Your translator automatically inherits all security features from `SchemaTranslator`.

### Step 6: Use in Search

Once the provider is registered, books will:

1. **Automatically merge** with duplicates from other providers
2. **Appear in search results** with unified metadata
3. **Show all sources** for user selection
4. **Get ranked** by quality score

## Complete Example: Internet Archive

Let me create a real example:
