import { useState, useEffect, useCallback } from "react";
import type { BookSearchResult } from "../types";

interface CoverCache {
  [canonicalId: string]: string | null;
}

const coverCache: CoverCache = {};

// Fetch cover from OpenLibrary by searching
export async function fetchOpenLibraryCover(
  title: string,
  author?: string
): Promise<string | null> {
  try {
    // Search OpenLibrary for the book
    const query = encodeURIComponent(`${title} ${author || ""}`.trim());
    const response = await fetch(
      `https://openlibrary.org/search.json?q=${query}&limit=1&fields=cover_i,isbn,key,title`
    );
    if (!response.ok) return null;

    const data = await response.json();
    if (data.docs && data.docs.length > 0) {
      const doc = data.docs[0];
      // Try to get cover from the search result using cover_i
      if (doc.cover_i) {
        return `https://covers.openlibrary.org/b/id/${doc.cover_i}-M.jpg`;
      }
      // Try ISBN
      if (doc.isbn && doc.isbn.length > 0) {
        return `https://covers.openlibrary.org/b/isbn/${doc.isbn[0]}-M.jpg?default=false`;
      }
      // Try OLID from key
      if (doc.key) {
        const olid = doc.key.replace("/works/", "");
        if (olid.startsWith("OL")) {
          return `https://covers.openlibrary.org/b/olid/${olid}-M.jpg?default=false`;
        }
      }
    }
    return null;
  } catch (e) {
    console.warn("Failed to fetch OpenLibrary cover:", e);
    return null;
  }
}

// Extract work ID from OpenLibrary URL
export function extractOpenLibraryWorkId(url: string): string | null {
  const match = url.match(/\/works\/(OL\d+W)/i);
  if (match) return match[1];
  return null;
}

// Check if URL is from OpenLibrary
export function isOpenLibraryUrl(url: string): boolean {
  return url.includes("openlibrary.org");
}

// Get enhanced cover URL with fallback to OpenLibrary
export async function getEnhancedCoverUrl(
  book: BookSearchResult,
  existingUrl: string | null
): Promise<string | null> {
  // If we already have a valid URL, use it
  if (existingUrl) {
    return existingUrl;
  }

  // Check cache first
  if (coverCache[book.canonical_id] !== undefined) {
    return coverCache[book.canonical_id];
  }

  // Check if book has OpenLibrary source
  const olSource =
    book.all_sources?.find((s) => s.provider === "openlibrary") ||
    (isOpenLibraryUrl(book.primary_source?.url || "")
      ? book.primary_source
      : null);

  if (olSource) {
    // Try to get cover from OpenLibrary work ID
    const workId = extractOpenLibraryWorkId(olSource.url);
    if (workId) {
      const coverUrl = `https://covers.openlibrary.org/b/olid/${workId}-M.jpg?default=false`;
      coverCache[book.canonical_id] = coverUrl;
      return coverUrl;
    }

    // Try using OLID from book_id
    if (olSource.book_id && olSource.book_id.startsWith("OL")) {
      const coverUrl = `https://covers.openlibrary.org/b/olid/${olSource.book_id}-M.jpg?default=false`;
      coverCache[book.canonical_id] = coverUrl;
      return coverUrl;
    }
  }

  // Try to fetch cover from OpenLibrary search API
  const authorName = book.authors?.[0]?.name;
  const searchCover = await fetchOpenLibraryCover(book.title, authorName);
  if (searchCover) {
    coverCache[book.canonical_id] = searchCover;
    return searchCover;
  }

  // Cache the null result to avoid repeated failed requests
  coverCache[book.canonical_id] = null;
  return null;
}

// Hook to manage enhanced covers
export function useOpenLibraryCovers(books: BookSearchResult[]) {
  const [enhancedCovers, setEnhancedCovers] = useState<{
    [canonicalId: string]: string | null;
  }>({});

  useEffect(() => {
    // Process each book that doesn't have a cached cover
    const fetchMissingCovers = async () => {
      const updates: { [canonicalId: string]: string | null } = {};

      for (const book of books) {
        // Skip if already cached
        if (coverCache[book.canonical_id] !== undefined) {
          if (coverCache[book.canonical_id]) {
            updates[book.canonical_id] = coverCache[book.canonical_id];
          }
          continue;
        }

        // Check if book already has a cover URL
        if (book.cover_url || book.thumbnail_url) {
          coverCache[book.canonical_id] =
            book.thumbnail_url || book.cover_url || null;
          continue;
        }

        // Check if it's an OpenLibrary book
        const hasOpenLibrary =
          book.all_sources?.some((s) => s.provider === "openlibrary") ||
          book.primary_source?.provider === "openlibrary" ||
          book.canonical_id.startsWith("openlibrary:");

        if (hasOpenLibrary) {
          const cover = await getEnhancedCoverUrl(book, null);
          if (cover) {
            updates[book.canonical_id] = cover;
          }
        }
      }

      if (Object.keys(updates).length > 0) {
        setEnhancedCovers((prev) => ({ ...prev, ...updates }));
      }
    };

    fetchMissingCovers();
  }, [books]);

  return enhancedCovers;
}
