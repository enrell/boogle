import json
from pathlib import Path

_STOPWORDS: frozenset[str] | None = None


def load_stopwords() -> frozenset[str]:
    global _STOPWORDS
    if _STOPWORDS is not None:
        return _STOPWORDS
    
    stopwords_file = Path(__file__).parent.parent.parent / "stopwords-iso.json"
    if not stopwords_file.exists():
        _STOPWORDS = frozenset()
        return _STOPWORDS
    
    with open(stopwords_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    all_words: set[str] = set()
    for lang_words in data.values():
        all_words.update(w.lower() for w in lang_words)
    
    _STOPWORDS = frozenset(all_words)
    return _STOPWORDS
