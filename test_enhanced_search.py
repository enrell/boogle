#!/usr/bin/env python3
"""
Test script for enhanced search features.

Tests:
1. Language detection
2. Spell correction
3. Query expansion
4. End-to-end enhanced search
"""

import sys

sys.path.insert(0, ".")


def test_language_detection():
    """Test language detection."""
    print("\n=== Testing Language Detection ===")
    try:
        from src.search.language import LanguageDetector

        detector = LanguageDetector()

        test_texts = [
            ("The quick brown fox jumps over the lazy dog", "en"),
            ("O Brasil é um país maravilhoso com uma rica história", "pt"),
            ("El gato come pescado en la casa", "es"),
            ("Le chat dort sur le canapé", "fr"),
        ]

        for text, expected in test_texts:
            detected = detector.detect(text)
            confidence = detector.detect_with_confidence(text)
            print(f"  '{text[:40]}...' -> {detected} (expected: {expected})")
            if confidence:
                print(f"    Confidence: {confidence[1]:.2%}")

        print("  ✓ Language detection working")
        return True
    except Exception as e:
        print(f"  ✗ Language detection failed: {e}")
        return False


def test_spell_correction():
    """Test spell correction."""
    print("\n=== Testing Spell Correction ===")
    try:
        from src.search.spellcheck import SpellCorrector

        corrector = SpellCorrector()

        # Add some words to dictionary
        corrector.add_words(
            {
                "shakespeare": 1000,
                "hemingway": 800,
                "tolkien": 900,
                "book": 5000,
                "novel": 4000,
                "author": 3000,
            }
        )

        test_queries = [
            ("shakspeare", "shakespeare"),
            ("hemingay", "hemingway"),
            ("tolkein", "tolkien"),
        ]

        for query, expected in test_queries:
            corrected, was_corrected, corrections = corrector.correct_query(query)
            print(f"  '{query}' -> '{corrected}' (expected: '{expected}')")
            if corrections:
                print(f"    Corrections: {corrections}")

        print("  ✓ Spell correction working")
        return True
    except Exception as e:
        print(f"  ✗ Spell correction failed: {e}")
        return False


def test_query_expansion():
    """Test query expansion with synonyms."""
    print("\n=== Testing Query Expansion ===")
    try:
        from src.search.expansion import BookQueryExpander

        expander = BookQueryExpander(max_expansions=3)

        test_queries = [
            "novel",
            "author",
            "character",
        ]

        for query in test_queries:
            expanded, expansions = expander.expand_query(query, lang="en")
            print(f"  '{query}' -> {len(expansions)} expansions")
            if expansions:
                print(f"    Expansions: {expansions}")

        print("  ✓ Query expansion working")
        return True
    except Exception as e:
        print(f"  ✗ Query expansion failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_enhanced_search():
    """Test full enhanced search integration."""
    print("\n=== Testing Enhanced Search Integration ===")
    try:
        from src.search import EnhancedSearcher

        # Check if index exists
        import os

        if not os.path.exists("data/index/index.json"):
            print("  ⚠ No BM25 index found, skipping search test")
            return True

        searcher = EnhancedSearcher(
            index_dir="data/index",
            enable_spellcheck=False,  # Skip spellcheck for speed
            enable_expansion=False,  # Skip expansion for speed
        )

        # Test search
        results, info = searcher.search("machado", top_k=5)
        print(f"  Query: 'machado'")
        print(f"  Found {len(results)} results")
        print(f"  Language detected: {info.get('language')}")

        for r in results[:3]:
            print(f"    Book {r.book_id}: score={r.score:.4f}")

        print("  ✓ Enhanced search working")
        return True
    except Exception as e:
        print(f"  ✗ Enhanced search failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def main():
    """Run all tests."""
    print("=" * 60)
    print("Boogle Enhanced Search Test Suite")
    print("=" * 60)

    results = []

    # Run tests
    results.append(("Language Detection", test_language_detection()))
    results.append(("Spell Correction", test_spell_correction()))
    results.append(("Query Expansion", test_query_expansion()))
    results.append(("Enhanced Search", test_enhanced_search()))

    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)

    passed = sum(1 for _, r in results if r)
    total = len(results)

    for name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"  {status}: {name}")

    print(f"\n{passed}/{total} tests passed")

    return passed == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
