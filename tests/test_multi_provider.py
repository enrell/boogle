#!/usr/bin/env python3
"""
Test multi-provider incremental indexing.

This script tests the new BookSeeder with multiple providers.
"""

import sys

sys.path.insert(0, "/home/kokoro/boogle")

from src.providers.registry import ProviderRegistry
from src.downloader.downloader import BookSeeder


def test_provider_discovery():
    """Test that providers are discovered."""
    print("=" * 60)
    print("Test 1: Provider Discovery")
    print("=" * 60)

    ProviderRegistry.auto_discover()
    providers = ProviderRegistry.list_sources()

    print(f"Found {len(providers)} providers:")
    for name in providers:
        print(f"  - {name}")

    assert len(providers) > 0, "No providers found!"
    print("✓ Provider discovery working\n")
    return providers


def test_provider_instances():
    """Test that we can instantiate providers."""
    print("=" * 60)
    print("Test 2: Provider Instantiation")
    print("=" * 60)

    providers = []
    for name in ["gutenberg", "openlibrary", "pportal"]:
        try:
            provider = ProviderRegistry.get(name)
            providers.append(provider)
            print(f"  ✓ {name}: {provider.source_name}")
        except ValueError as e:
            print(f"  ✗ {name}: {e}")

    assert len(providers) > 0, "No providers could be instantiated!"
    print(f"✓ Instantiated {len(providers)} providers\n")
    return providers


def test_book_seeder_init():
    """Test BookSeeder initialization."""
    print("=" * 60)
    print("Test 3: BookSeeder Initialization")
    print("=" * 60)

    providers = []
    for name in ["gutenberg", "openlibrary", "pportal"]:
        try:
            p = ProviderRegistry.get(name)
            providers.append(p)
        except:
            pass

    if not providers:
        print("No providers available, skipping\n")
        return None

    seeder = BookSeeder(
        providers=providers,
        output_dir="data/test_books",
        max_workers=4,
        use_sqlite=True,
        light_mode=True,  # Don't download files
    )

    print(f"  - Providers: {[p.source_name for p in seeder.providers]}")
    print(f"  - Light mode: {seeder.light_mode}")
    print(f"  - Max workers: {seeder.max_workers}")
    print("✓ BookSeeder initialized\n")

    return seeder


def test_checkpoint_system(seeder):
    """Test database checkpoint system."""
    print("=" * 60)
    print("Test 4: Database Checkpoint System")
    print("=" * 60)

    for provider in seeder.providers:
        # Get initial offset
        position, last_book = seeder._get_provider_offset(provider)
        print(f"  - {provider.source_name}: position={position}, last_book={last_book}")

        # Test updating offset
        seeder._update_provider_offset(provider, 100, "test_book_123")
        position2, last_book2 = seeder._get_provider_offset(provider)

        assert position2 == 100, "Offset not updated!"
        assert last_book2 == "test_book_123", "Last book not updated!"
        print(f"    ✓ Updated to position={position2}, last_book={last_book2}")

        # Reset for clean state
        seeder._update_provider_offset(provider, -1, None)

    print("✓ Checkpoint system working\n")


def test_incremental_seeding(seeder):
    """Test incremental seeding."""
    print("=" * 60)
    print("Test 5: Incremental Seeding (Light Mode)")
    print("=" * 60)

    # Seed with limit of 2 books per provider
    print("Seeding with limit=2 per provider...")
    results = seeder.seed_all(
        limit=2,
        batch_size=10,
        cross_reference=False,  # Skip for speed
    )

    print(f"\nSeeding results:")
    for name, count in results.items():
        print(f"  - {name}: {count} books")

    total = sum(results.values())
    print(f"\nTotal: {total} books seeded")

    if total > 0:
        print("✓ Incremental seeding working\n")
    else:
        print("⚠ No books seeded (may need to check provider connectivity)\n")

    # Show checkpoints
    print("Checkpoint status after seeding:")
    for provider in seeder.providers:
        position, last_book = seeder._get_provider_offset(provider)
        print(f"  - {provider.source_name}: position={position}, last_book={last_book}")

    return results


def test_second_run(seeder):
    """Test that second run finds no new books (already seeded)."""
    print("=" * 60)
    print("Test 6: Second Run (No New Books)")
    print("=" * 60)

    print("Running again with limit=2...")
    results2 = seeder.seed_all(
        limit=2,
        batch_size=10,
        cross_reference=False,
    )

    print(f"\nSecond run results:")
    for name, count in results2.items():
        print(f"  - {name}: {count} books")

    total2 = sum(results2.values())
    print(f"\nTotal: {total2} books (should be 0 or small)")

    if total2 == 0:
        print("✓ Incremental seeding correctly skipped existing books\n")
    else:
        print("⚠ Some books were re-seeded (may need to check deduplication)\n")


def main():
    """Run all tests."""
    print("\n" + "=" * 60)
    print("Multi-Provider Incremental Indexing Tests")
    print("=" * 60 + "\n")

    try:
        # Test 1: Provider discovery
        provider_names = test_provider_discovery()

        # Test 2: Provider instantiation
        providers = test_provider_instances()

        # Test 3: BookSeeder initialization
        seeder = test_book_seeder_init()

        if seeder is None:
            print("\nSkipping remaining tests (no seeder available)\n")
            return

        # Test 4: Checkpoint system
        test_checkpoint_system(seeder)

        # Test 5: Incremental seeding
        results = test_incremental_seeding(seeder)

        # Test 6: Second run
        test_second_run(seeder)

        # Cleanup
        seeder.close()

        print("=" * 60)
        print("All tests completed!")
        print("=" * 60 + "\n")

    except Exception as e:
        print(f"\n✗ Test failed with error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
