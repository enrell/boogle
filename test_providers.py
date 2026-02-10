#!/usr/bin/env python3
"""
Test script for verifying provider implementations.

Usage:
    python test_providers.py
    python test_providers.py --test-openlibrary
    python test_providers.py --test-bndigital
"""

import sys
import argparse

# Ensure src is in path
sys.path.insert(0, "/home/kokoro/boogle")


def test_provider_registration():
    """Test that providers are registered correctly."""
    print("=" * 60)
    print("Testing Provider Registration")
    print("=" * 60)

    from src.providers.registry import ProviderRegistry

    # Auto-discover providers
    ProviderRegistry.auto_discover()

    sources = ProviderRegistry.list_sources()
    print(f"\nRegistered providers: {sources}")

    # Check required providers
    required = ["gutenberg", "openlibrary", "bndigital", "example"]
    missing = [r for r in required if r not in sources]

    if missing:
        print(f"\n✗ Missing providers: {missing}")
        return False

    print(f"\n✓ All expected providers registered: {sources}")
    return True


def test_gutenberg_provider():
    """Test Gutenberg provider."""
    print("\n" + "=" * 60)
    print("Testing Gutenberg Provider")
    print("=" * 60)

    try:
        from src.providers.registry import ProviderRegistry

        provider = ProviderRegistry.get("gutenberg")
        print(f"✓ Got provider: {provider.source_name}")
        print(f"✓ Supports downloads: {provider.supports_downloads()}")
        print(f"✓ Enabled by default: {provider.enabled_by_default}")

        # Test metadata extraction (limited test)
        print("\nTesting metadata extraction (book ID: 1)...")
        meta = provider.extract_metadata("1")

        if meta.get("title"):
            print(f"✓ Title: {meta['title']}")
        else:
            print("⚠ No title (may be network issue)")

        if meta.get("author"):
            print(f"✓ Author: {meta['author']}")

        print(f"✓ URL: {meta.get('url')}")

        # Test iterator (just first item)
        print("\nTesting iterator (first book only)...")
        for book in provider.iter_book_metadata(limit=1):
            print(f"✓ Got book: {book.get('title', 'N/A')}")
            break
        else:
            print("⚠ No books returned (may be network issue)")

        return True

    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_openlibrary_provider():
    """Test Open Library provider."""
    print("\n" + "=" * 60)
    print("Testing Open Library Provider")
    print("=" * 60)

    try:
        from src.providers.registry import ProviderRegistry

        provider = ProviderRegistry.get("openlibrary")
        print(f"✓ Got provider: {provider.source_name}")
        print(f"✓ Supports downloads: {provider.supports_downloads()}")
        print(f"✓ Enabled by default: {provider.enabled_by_default}")

        # Test search
        print("\nTesting search API (query: 'shakespeare')...")
        results = provider.search_books("shakespeare", limit=3)

        if results:
            print(f"✓ Found {len(results)} results")
            for i, book in enumerate(results[:2], 1):
                print(
                    f"  {i}. {book.get('title', 'N/A')} by {book.get('author', 'Unknown')}"
                )
        else:
            print("⚠ No search results (may be API issue)")

        # Test metadata extraction
        print("\nTesting metadata extraction (OL1W - The Jungle Book)...")
        meta = provider.extract_metadata("OL1W")

        if meta.get("title"):
            print(f"✓ Title: {meta['title']}")
        else:
            print("⚠ No title returned")

        if meta.get("author"):
            print(f"✓ Author: {meta.get('author')}")

        print(f"✓ URL: {meta.get('url')}")

        # Test iterator (first item only)
        print("\nTesting iterator (first book only)...")
        for book in provider.iter_book_metadata(limit=1):
            print(f"✓ Got book: {book.get('title', 'N/A')}")
            break
        else:
            print("⚠ No books returned (may be API issue)")

        return True

    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_pportal_provider():
    """Test PPORTAL provider."""
    print("\n" + "=" * 60)
    print("Testing PPORTAL Provider")
    print("=" * 60)

    try:
        from src.providers.registry import ProviderRegistry

        provider = ProviderRegistry.get("pportal")
        print(f"✓ Got provider: {provider.source_name}")
        print(f"✓ Supports downloads: {provider.supports_downloads()}")
        print(f"✓ Enabled by default: {provider.enabled_by_default}")

        # Show dataset info
        print("\nDataset information:")
        info = provider.get_dataset_info()
        print(f"  Total works: {info['total_works']}")
        print(f"  Cache dir: {info['cache_dir']}")

        # Test search (Portuguese term)
        print("\nTesting search API (query: 'Machado')...")
        results = provider.search_books("Machado", limit=3)

        if results:
            print(f"✓ Found {len(results)} results")
            for i, book in enumerate(results[:2], 1):
                print(f"  {i}. {book.get('title', 'N/A')}")
        else:
            print("⚠ No search results (dataset may need download)")

        # Test iterator (first item only)
        print("\nTesting iterator (first book only)...")
        for book in provider.iter_book_metadata(limit=1):
            print(f"✓ Got book: {book.get('title', 'N/A')}")
            if book.get("author"):
                print(f"  Author: {book['author']}")
            if book.get("category"):
                print(f"  Category: {book['category'][:60]}...")
            break
        else:
            print("⚠ No books returned (may need to download dataset first)")

        return True

    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_provider_interface_compliance():
    """Test that all providers implement the interface correctly."""
    print("\n" + "=" * 60)
    print("Testing Provider Interface Compliance")
    print("=" * 60)

    from src.providers.base import BaseBookProvider
    from src.providers.registry import ProviderRegistry

    ProviderRegistry.auto_discover()

    required_methods = [
        "source_name",
        "iter_book_metadata",
        "extract_metadata",
        "get_book_url",
        "filter_book",
    ]

    optional_methods = [
        "download_book",
        "get_cover_url",
        "get_format_priorities",
        "supports_downloads",
        "search_books",
        "get_seeder_config",
    ]

    all_pass = True

    for source in ProviderRegistry.list_sources():
        print(f"\nChecking {source}...")
        provider = ProviderRegistry.get(source)

        # Check required methods
        for method in required_methods:
            if method == "source_name":
                # Property, not method
                try:
                    value = getattr(provider, method)
                    print(f"  ✓ {method}: {value}")
                except Exception as e:
                    print(f"  ✗ {method}: {e}")
                    all_pass = False
            else:
                if hasattr(provider, method):
                    print(f"  ✓ {method}")
                else:
                    print(f"  ✗ {method}: missing")
                    all_pass = False

        # Check optional methods
        for method in optional_methods:
            if hasattr(provider, method):
                print(f"  ✓ {method} (optional)")
            else:
                print(f"  ⚠ {method} (optional): not implemented")

        # Check it's a subclass of BaseBookProvider
        if isinstance(provider, BaseBookProvider):
            print(f"  ✓ Inherits from BaseBookProvider")
        else:
            print(f"  ✗ Does not inherit from BaseBookProvider")
            all_pass = False

    return all_pass


def main():
    parser = argparse.ArgumentParser(description="Test Boogle providers")
    parser.add_argument(
        "--test-openlibrary",
        action="store_true",
        help="Test only Open Library provider",
    )
    parser.add_argument(
        "--test-bndigital", action="store_true", help="Test only BNDigital provider"
    )
    parser.add_argument(
        "--test-gutenberg", action="store_true", help="Test only Gutenberg provider"
    )
    parser.add_argument(
        "--skip-network", action="store_true", help="Skip network-dependent tests"
    )

    args = parser.parse_args()

    print("Boogle Provider Test Suite")
    print("=" * 60)

    results = []

    # Always test registration
    results.append(("Registration", test_provider_registration()))

    if args.skip_network:
        print("\nSkipping network tests (--skip-network)")
        # Just test interface compliance
        results.append(("Interface Compliance", test_provider_interface_compliance()))
    else:
        # Test specific providers or all
        if args.test_openlibrary:
            results.append(("Open Library", test_openlibrary_provider()))
        elif args.test_bndigital:
            results.append(("BNDigital", test_bndigital_provider()))
        elif args.test_gutenberg:
            results.append(("Gutenberg", test_gutenberg_provider()))
        else:
            # Test all
            results.append(("Gutenberg", test_gutenberg_provider()))
            results.append(("Open Library", test_openlibrary_provider()))
            results.append(("BNDigital", test_bndigital_provider()))
            results.append(
                ("Interface Compliance", test_provider_interface_compliance())
            )

    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)

    for name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{name}: {status}")

    all_passed = all(r[1] for r in results)

    print("=" * 60)
    if all_passed:
        print("\n✓ All tests passed!")
        return 0
    else:
        print("\n✗ Some tests failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
