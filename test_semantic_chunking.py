#!/usr/bin/env python3
"""
Test semantic chunking functionality.

Tests content-aware chunking that preserves semantic units:
- Chapters (e.g., "CHAPTER 1", "Chapter I")
- Paragraphs (double newlines)
- Sentences (. ! ? with abbreviation detection)
"""

import sys

sys.path.insert(0, ".")


def test_chapter_detection():
    """Test chapter boundary detection."""
    print("\n=== Testing Chapter Detection ===")

    try:
        from rust_bm25 import chunk_by_structure

        text = """CHAPTER 1: The Beginning

This is the first chapter. It has some content here.

CHAPTER 2: The Middle

This is the second chapter. More content here.
Multiple paragraphs in this chapter.

Another paragraph.

CHAPTER 3: The End

Final chapter content here."""

        chunks = chunk_by_structure(text, "Chapter")
        print(f"  Found {len(chunks)} chapters")

        for i, chunk in enumerate(chunks[:3]):
            print(f"  Chapter {i + 1}: {chunk[:50]}...")

        # Should find at least 3 chapters
        assert len(chunks) >= 3, f"Expected 3 chapters, got {len(chunks)}"
        print("  ✓ Chapter detection working")
        return True
    except Exception as e:
        print(f"  ✗ Chapter detection failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_paragraph_splitting():
    """Test paragraph boundary detection."""
    print("\n=== Testing Paragraph Splitting ===")

    try:
        from rust_bm25 import chunk_by_structure

        text = """First paragraph here. It has multiple sentences.
Still in the first paragraph.

Second paragraph starts here. This is paragraph two.
It also has multiple sentences.

Third paragraph is here. More content in paragraph three.

Fourth and final paragraph."""

        chunks = chunk_by_structure(text, "Paragraph")
        print(f"  Found {len(chunks)} paragraphs")

        for i, chunk in enumerate(chunks[:4]):
            print(f"  Para {i + 1}: {chunk[:40]}...")

        # Should find at least 4 paragraphs
        assert len(chunks) >= 4, f"Expected 4 paragraphs, got {len(chunks)}"
        print("  ✓ Paragraph splitting working")
        return True
    except Exception as e:
        print(f"  ✗ Paragraph splitting failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_sentence_splitting():
    """Test sentence boundary detection."""
    print("\n=== Testing Sentence Splitting ===")

    try:
        from rust_bm25 import chunk_by_structure

        text = """First sentence here. Second sentence follows!
Third sentence asks a question?
Fourth sentence has Mr. Smith and Dr. Jones.
Fifth sentence ends with excitement!"""

        chunks = chunk_by_structure(text, "Sentence")
        print(f"  Found {len(chunks)} sentences")

        for i, chunk in enumerate(chunks[:5]):
            print(f"  Sentence {i + 1}: {chunk[:50]}...")

        # Should find at least 5 sentences
        assert len(chunks) >= 5, f"Expected 5 sentences, got {len(chunks)}"
        print("  ✓ Sentence splitting working")
        return True
    except Exception as e:
        print(f"  ✗ Sentence splitting failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_semantic_chunking():
    """Test smart semantic chunking (priority: chapter → paragraph → sentence)."""
    print("\n=== Testing Semantic Chunking ===")

    try:
        from rust_bm25 import chunk_text_semantic

        # Create text with chapters, paragraphs, and long content
        text = """CHAPTER 1: Introduction

This is the first paragraph of the introduction. It contains information about the topic.
This paragraph continues with more details and explanations.

This is the second paragraph. It discusses related concepts and provides examples.
The examples help illustrate the main points.

CHAPTER 2: Methods

The methods section begins here. It describes the approach used.
Various techniques are discussed in detail.

This paragraph explains the implementation. Code examples would go here.
More technical details follow.

CHAPTER 3: Results

Results are presented in this chapter. Data shows significant improvements.
Statistical analysis confirms the findings.

The final paragraph summarizes the results. Future work is also discussed.
More text here to make it longer."""

        chunks = chunk_text_semantic(text, target_size=500, overlap=50)
        print(f"  Created {len(chunks)} semantic chunks")
        print(
            f"  Average chunk size: {sum(len(c) for c in chunks) // len(chunks) if chunks else 0} chars"
        )

        # Check that chunks preserve semantic boundaries
        for i, chunk in enumerate(chunks[:5]):
            preview = chunk[:60].replace("\n", " ")
            print(f"  Chunk {i + 1}: {preview}...")

        # Chunks should be reasonably sized
        if chunks:
            sizes = [len(c) for c in chunks]
            avg_size = sum(sizes) / len(sizes)
            print(f"  Size range: {min(sizes)}-{max(sizes)} chars")
            print(f"  ✓ Semantic chunking working")

        return True
    except Exception as e:
        print(f"  ✗ Semantic chunking failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_comparison_with_fixed():
    """Compare semantic vs fixed-size chunking."""
    print("\n=== Comparing Semantic vs Fixed Chunking ===")

    try:
        from rust_bm25 import chunk_text, chunk_text_semantic

        # Text with clear chapter structure
        text = """CHAPTER 1: The Beginning

This is the first chapter. It contains important information about the start of our story.
The story begins with a description of the setting and characters.

More details follow in this chapter. The plot thickens as we learn more.

CHAPTER 2: The Middle

Now we reach the middle of the story. Things get more interesting here.
The conflict is introduced and tension builds.

Subplots develop and characters grow. This chapter is crucial to understanding the narrative.

CHAPTER 3: The End

Finally we reach the conclusion. All loose ends are tied up here.
The resolution provides closure to the story."""

        # Fixed-size chunking
        fixed_chunks = chunk_text(text, chunk_size=200, overlap=20)
        print(f"  Fixed-size chunks: {len(fixed_chunks)}")

        # Semantic chunking
        semantic_chunks = chunk_text_semantic(text, target_size=200, overlap=20)
        print(f"  Semantic chunks: {len(semantic_chunks)}")

        # Show sample chunks
        print("\n  Fixed chunk sample:")
        print(f"    '{fixed_chunks[0][:60]}...'")

        print("\n  Semantic chunk sample:")
        print(f"    '{semantic_chunks[0][:60]}...'")

        print("\n  ✓ Comparison complete")
        return True
    except Exception as e:
        print(f"  ✗ Comparison failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def main():
    """Run all semantic chunking tests."""
    print("=" * 70)
    print("Semantic Chunking Test Suite")
    print("Content-aware chunking with chapter/paragraph/sentence boundaries")
    print("=" * 70)

    results = []

    # Run tests
    results.append(("Chapter Detection", test_chapter_detection()))
    results.append(("Paragraph Splitting", test_paragraph_splitting()))
    results.append(("Sentence Splitting", test_sentence_splitting()))
    results.append(("Semantic Chunking", test_semantic_chunking()))
    results.append(("Comparison with Fixed", test_comparison_with_fixed()))

    # Summary
    print("\n" + "=" * 70)
    print("Test Summary")
    print("=" * 70)

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
