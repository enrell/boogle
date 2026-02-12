import json
from pathlib import Path
from typing import Optional, Set, FrozenSet

_STOPWORDS_CACHE: dict[str, FrozenSet[str]] = {}
_ALL_STOPWORDS: FrozenSet[str] | None = None


def load_stopwords(languages: Optional[list[str]] = None) -> FrozenSet[str]:
    """
    Load stopwords for specified languages.

    Args:
        languages: List of ISO language codes (e.g., ['en', 'pt', 'es']).
                  If None, returns only English stopwords for backward compatibility.

    Returns:
        Frozen set of stopwords
    """
    global _ALL_STOPWORDS

    if languages is None:
        # Backward compatibility: load only English
        languages = ["en"]

    stopwords_file = Path(__file__).parent.parent.parent / "stopwords-iso.json"
    if not stopwords_file.exists():
        return frozenset()

    # Build cache key
    cache_key = ",".join(sorted(languages))

    if cache_key in _STOPWORDS_CACHE:
        return _STOPWORDS_CACHE[cache_key]

    with open(stopwords_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    all_words: Set[str] = set()
    for lang in languages:
        if lang in data:
            all_words.update(w.lower() for w in data[lang])

    result = frozenset(all_words)
    _STOPWORDS_CACHE[cache_key] = result

    return result


def get_stopwords_for_language(lang: str) -> FrozenSet[str]:
    """
    Get stopwords for a specific language.

    Args:
        lang: ISO 639-1 language code (e.g., 'en', 'pt', 'es')

    Returns:
        Frozen set of stopwords for that language
    """
    return load_stopwords([lang])


def get_supported_languages() -> list[str]:
    """
    Get list of supported language codes.

    Returns:
        List of ISO 639-1 language codes
    """
    stopwords_file = Path(__file__).parent.parent.parent / "stopwords-iso.json"
    if not stopwords_file.exists():
        return ["en"]

    with open(stopwords_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    return sorted(data.keys())


def is_stopword(word: str, lang: Optional[str] = None) -> bool:
    """
    Check if a word is a stopword.

    Args:
        word: Word to check
        lang: Language code. If None, checks against all loaded stopwords.

    Returns:
        True if word is a stopword
    """
    word = word.lower()

    if lang:
        stopwords = get_stopwords_for_language(lang)
        return word in stopwords
    else:
        # Check against all supported languages
        all_langs = get_supported_languages()
        all_stopwords = load_stopwords(all_langs)
        return word in all_stopwords


# Backward compatibility: module-level stopwords
_STOPWORDS: FrozenSet[str] | None = None


def _load_english_stopwords() -> FrozenSet[str]:
    """Load English stopwords for backward compatibility."""
    global _STOPWORDS
    if _STOPWORDS is not None:
        return _STOPWORDS

    stopwords_file = Path(__file__).parent.parent.parent / "stopwords-iso.json"
    if not stopwords_file.exists():
        _STOPWORDS = frozenset()
        return _STOPWORDS

    with open(stopwords_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    all_words: Set[str] = set()
    if "en" in data:
        all_words.update(w.lower() for w in data["en"])

    _STOPWORDS = frozenset(all_words)
    return _STOPWORDS


# Maintain backward compatibility
if _STOPWORDS is None:
    _STOPWORDS = _load_english_stopwords()
