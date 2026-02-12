import { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import type { MetaFunction, LoaderFunctionArgs } from "react-router";
import { useLoaderData, useFetcher, useSearchParams, Link } from "react-router";
import { Header } from "../components/layout/Header";
import { Footer } from "../components/layout/Footer";
import { searchBooksDetailed } from "../api.server";
import type { BookSearchResult, BookMetadata } from "../types";
import { BookDetailsPanel } from "../components/features/book/BookDetailsPanel";
import { BookDetailsModal } from "../components/features/book/BookDetailsModal";
import { CachedImage } from "../components/ui/CachedImage";
import { useOpenLibraryCovers } from "../hooks/useOpenLibraryCovers";

// Helper to parse canonical_id (e.g., "gutenberg:1524" -> { provider: "gutenberg", bookId: "1524" })
function parseCanonicalId(canonicalId: string): { provider: string; bookId: string } {
  const [provider, ...rest] = canonicalId.split(":");
  return { provider, bookId: rest.join(":") };
}

// Extract OpenLibrary work ID from URL
function extractOpenLibraryWorkId(url: string): string | null {
  const match = url.match(/\/works\/(OL\d+W)/i);
  if (match) return match[1];
  return null;
}

// Helper to construct cover URL based on provider
function getCoverUrl(book: BookSearchResult): string | null {
  // Use thumbnail from API if available
  if (book.thumbnail_url) {
    return book.thumbnail_url;
  }
  
  // Try to get cover from primary source
  if (book.cover_url) {
    return book.cover_url;
  }
  
  const { provider, bookId } = parseCanonicalId(book.canonical_id);
  
  if (provider === "gutenberg") {
    return `https://www.gutenberg.org/cache/epub/${bookId}/pg${bookId}.cover.medium.jpg`;
  }
  
  // OpenLibrary covers - try multiple approaches
  if (provider === "openlibrary" || book.primary_source?.provider === "openlibrary") {
    // Try to extract work ID from URL
    const workId = extractOpenLibraryWorkId(book.primary_source?.url || "");
    if (workId) {
      return `https://covers.openlibrary.org/b/olid/${workId}-M.jpg?default=false`;
    }
    
    // Try using the book_id directly
    const olid = book.primary_source?.book_id || bookId;
    if (olid && olid.startsWith("OL")) {
      return `https://covers.openlibrary.org/b/olid/${olid}-M.jpg?default=false`;
    }
    
    // Check all_sources for OpenLibrary
    const olSource = book.all_sources?.find(s => s.provider === "openlibrary");
    if (olSource) {
      const olWorkId = extractOpenLibraryWorkId(olSource.url);
      if (olWorkId) {
        return `https://covers.openlibrary.org/b/olid/${olWorkId}-M.jpg?default=false`;
      }
      if (olSource.book_id && olSource.book_id.startsWith("OL")) {
        return `https://covers.openlibrary.org/b/olid/${olSource.book_id}-M.jpg?default=false`;
      }
    }
  }
  
  return null;
}

export const meta: MetaFunction = () => {
  return [
    { title: "Boogle Search" },
    { name: "description", content: "Search results for free books" },
  ];
};

type LoaderData = {
  query: string;
  results: BookSearchResult[];
  error: string | null;
  searchTime: number;
  page: number;
};

export async function loader({ request }: LoaderFunctionArgs): Promise<LoaderData> {
  const url = new URL(request.url);
  const page = parseInt(url.searchParams.get("page") || "1", 10);
  const query = url.searchParams.get("q") || "";

  if (!query.trim()) {
    return { query, results: [], error: null, searchTime: 0, page };
  }

  const startTime = Date.now();
  try {
    const response = await searchBooksDetailed(query, page);
    return {
      query,
      results: response.results,
      error: null,
      searchTime: Date.now() - startTime,
      page,
    };
  } catch (err) {
    return {
      query,
      results: [],
      error: (err as Error).message,
      searchTime: Date.now() - startTime,
      page,
    };
  }
}

export default function Search() {
  const { query, results, error, searchTime, page } = useLoaderData<typeof loader>();
  const [searchParams, setSearchParams] = useSearchParams();
  const fetcher = useFetcher<BookMetadata>();
  const { t } = useTranslation();
  
  // Use OpenLibrary covers enhancement
  const enhancedCovers = useOpenLibraryCovers(results);

  const previewId = searchParams.get("preview");
  const isPanelOpen = !!previewId;

  useEffect(() => {
    if (previewId) {
      fetcher.load(`/api/book/${previewId}`);
    }
  }, [previewId]);

  // Image Pre-loading Effect - use enhanced covers when available
  useEffect(() => {
    if (results.length > 0) {
      results.forEach((book) => {
        // Try enhanced cover first, then fall back to basic
        const coverUrl = enhancedCovers[book.canonical_id] || getCoverUrl(book);
        if (coverUrl) {
          const proxyUrl = `/api/image-proxy?url=${encodeURIComponent(coverUrl)}`;
          fetch(proxyUrl, { priority: "low" } as any).catch(() => { });
        }
      });
    }
  }, [results, enhancedCovers]);

  const handleBookClick = (e: React.MouseEvent, bookId: string) => {
    e.preventDefault();
    setSearchParams(prev => {
      prev.set("preview", bookId);
      return prev;
    });
  };

  const closePanel = () => {
    setSearchParams(prev => {
      prev.delete("preview");
      return prev;
    });
  };

  return (
    <div className="flex flex-col min-h-screen bg-white dark:bg-boogle-dark">
      <Header query={query} />

      <div className="flex flex-1 relative overflow-hidden">
        {/* Main Results Area */}
        <main className="flex-1 overflow-y-auto px-4 py-6 md:px-8">
          <div className="max-w-7xl w-full">
            {error && (
              <div className="bg-red-50 border border-red-200 rounded-lg p-4 mb-6">
                <p className="text-red-700 font-medium">{error}</p>
              </div>
            )}

            {!error && results.length > 0 && (
              <p className="text-gray-500 text-sm mb-6">
                {t('about_results', { count: results.length })} ({(searchTime / 1000).toFixed(2)} {t('seconds')})
              </p>
            )}

            {!error && results.length === 0 && query && (
              <div className="text-gray-600 dark:text-gray-400">
                <p className="text-lg mb-2">{t('no_results', { query })}</p>
                <p className="text-sm">{t('try_different_keywords')}</p>
              </div>
            )}

            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 xl:grid-cols-6 gap-4 pb-10">
              {results.map((book) => {
                // Use enhanced cover from OpenLibrary search if available
                const enhancedCover = enhancedCovers[book.canonical_id];
                const basicCover = getCoverUrl(book);
                const coverUrl = enhancedCover || basicCover;
                
                const authorString = book.authors?.length 
                  ? book.authors.map(a => a.name).join(", ")
                  : t('unknown_author');

                return (
                  <article
                    key={book.canonical_id}
                    className="group bg-white dark:bg-boogle-dark-lighter border border-gray-200 dark:border-white/10 rounded-xl shadow-sm hover:shadow-lg transition-all duration-200 overflow-hidden cursor-pointer flex flex-col"
                    onClick={(e) => handleBookClick(e as any, book.canonical_id)}
                  >
                    {/* Cover Image */}
                    <div className="relative aspect-[2/3] bg-gray-100 dark:bg-white/5 overflow-hidden">
                      {coverUrl ? (
                        <CachedImage
                          src={coverUrl}
                          alt={`Cover of ${book.title}`}
                          className="w-full h-full object-cover group-hover:scale-105 transition-transform duration-300"
                        />
                      ) : (
                        <div className="w-full h-full flex flex-col items-center justify-center text-gray-400 dark:text-gray-600 p-4">
                          <svg className="w-12 h-12 mb-2 opacity-50" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M12 6.253v13m0-13C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253m0-13C13.168 5.477 14.754 5 16.5 5c1.747 0 3.332.477 4.5 1.253v13C19.832 18.477 18.247 18 16.5 18c-1.746 0-3.332.477-4.5 1.253" />
                          </svg>
                          <span className="text-xs text-center">{t('no_preview')}</span>
                        </div>
                      )}
                      
                      {/* Source Badge */}
                      <div className="absolute top-2 right-2">
                        <span className="text-xs bg-black/50 text-white px-2 py-1 rounded-full backdrop-blur-sm">
                          {book.primary_source?.provider || 'unknown'}
                        </span>
                      </div>
                    </div>

                    {/* Book Info */}
                    <div className="p-3 flex flex-col flex-1">
                      <h3 className="text-sm font-semibold text-gray-900 dark:text-white mb-1 line-clamp-2 leading-tight">
                        {book.title || "Untitled"}
                      </h3>
                      <p className="text-xs text-gray-600 dark:text-gray-400 line-clamp-1 mb-2">
                        {authorString}
                      </p>
                      
                      {/* Year and Language */}
                      {(book.publication_year || book.language) && (
                        <div className="flex items-center gap-2 text-xs text-gray-500 dark:text-gray-500 mt-auto">
                          {book.publication_year && (
                            <span>{book.publication_year}</span>
                          )}
                          {book.publication_year && book.language && (
                            <span>·</span>
                          )}
                          {book.language && (
                            <span className="uppercase">{book.language}</span>
                          )}
                        </div>
                      )}
                    </div>
                  </article>
                );
              })}
            </div>

            {/* Pagination Controls */}
            {results.length > 0 && (
              <div className="flex justify-between items-center py-8 border-t border-gray-100 dark:border-white/5">
                {page > 1 ? (
                  <Link
                    to={`/search?q=${encodeURIComponent(query)}&page=${page - 1}`}
                    className="px-4 py-2 text-blue-600 dark:text-blue-400 hover:bg-blue-50 dark:hover:bg-white/5 rounded-md transition-colors font-medium border border-transparent hover:border-blue-100 dark:hover:border-white/10"
                  >
                    ← {t('previous')}
                  </Link>
                ) : <div></div>}

                <div className="flex gap-2 text-sm text-gray-500">
                  <span className="font-medium text-gray-900 dark:text-gray-300">Page {page}</span>
                </div>

                {results.length === 20 && (
                  <Link
                    to={`/search?q=${encodeURIComponent(query)}&page=${page + 1}`}
                    className="px-4 py-2 text-blue-600 dark:text-blue-400 hover:bg-blue-50 dark:hover:bg-white/5 rounded-md transition-colors font-medium border border-transparent hover:border-blue-100 dark:hover:border-white/10"
                  >
                    {t('next')} →
                  </Link>
                )}
              </div>
            )}
          </div>
        </main>

      {/* Modal */}
      {isPanelOpen && (
        <BookDetailsModal
          book={fetcher.data!}
          isLoading={!fetcher.data || fetcher.state === "loading"}
          onClose={closePanel}
        />
      )}
    </div>

    <Footer />
  </div>
  );
}
