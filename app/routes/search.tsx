import { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import type { MetaFunction, LoaderFunctionArgs } from "react-router";
import { useLoaderData, useFetcher, useSearchParams, Link } from "react-router";
import { Header } from "../components/layout/Header";
import { Footer } from "../components/layout/Footer";
import { searchBooks } from "../api.server";
import { type SearchResult, type BookMetadata } from "../types";
import { BookDetailsPanel } from "../components/features/book/BookDetailsPanel";

export const meta: MetaFunction = () => {
  return [
    { title: "Boogle Search" },
    { name: "description", content: "Search results for free books" },
  ];
};

type LoaderData = {
  query: string;
  results: SearchResult[];
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
    const results = await searchBooks(query, page);
    return {
      query,
      results,
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

  const previewId = searchParams.get("preview");
  const isPanelOpen = !!previewId;

  useEffect(() => {
    if (previewId) {
      fetcher.load(`/api/book/${previewId}`);
    }
  }, [previewId]);

  // Image Pre-loading Effect
  useEffect(() => {
    if (results.length > 0) {
      results.forEach((book) => {
        // Construct likely Gutenberg cover URL for pre-loading
        const coverUrl = `https://www.gutenberg.org/cache/epub/${book.book_id}/pg${book.book_id}.cover.medium.jpg`;
        const proxyUrl = `/api/image-proxy?url=${encodeURIComponent(coverUrl)}`;

        // Use a detached fetch to pre-load into browser cache
        // We use the proxy directly to ensure it gets cached with the right headers
        fetch(proxyUrl, { priority: "low" } as any).catch(() => { });
      });
    }
  }, [results]);

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
        <main className={`flex-1 overflow-y-auto px-4 py-6 md:pl-44 transition-all duration-300 ${isPanelOpen ? 'pr-[450px]' : ''}`}>
          <div className="max-w-2xl w-full">
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

            <div className="space-y-8 pb-10">
              {results.map((book) => (
                <article key={book.book_id} className="group">
                  <div className="flex items-center gap-2 text-sm text-gray-600 dark:text-gray-400 mb-1">
                    <div className="flex flex-col">
                      <span className="text-xs text-gray-900 dark:text-gray-200 font-medium line-clamp-1">{book.title || "Untitled"}</span>
                      <span className="text-xs text-gray-500 dark:text-gray-500 line-clamp-1">{book.url}</span>
                    </div>
                  </div>
                  <a
                    href={`/book/${book.book_id}`}
                    onClick={(e) => handleBookClick(e, book.book_id)}
                    className="text-xl text-blue-600 dark:text-blue-400 hover:underline visited:text-purple-700 dark:visited:text-purple-400 font-medium block mb-1 cursor-pointer"
                  >
                    {book.title || "Untitled"}
                  </a>
                  <p className="text-sm text-gray-700 dark:text-gray-300 line-clamp-2">
                    <span className="text-gray-500">{t('by')}</span>{" "}
                    <span className="font-medium">{book.author || t('unknown_author')}</span>
                  </p>
                </article>
              ))}
            </div>

            {/* Pagination Controls */}
            {results.length > 0 && (
              <div className="flex justify-between items-center py-8 border-t border-gray-100 dark:border-white/5">
                {page > 1 ? (
                  <Link
                    to={`/search?q=${encodeURIComponent(query)}&page=${page - 1}`}
                    className="px-4 py-2 text-blue-600 dark:text-blue-400 hover:bg-blue-50 dark:hover:bg-white/5 rounded-md transition-colors font-medium border border-transparent hover:border-blue-100 dark:hover:border-white/10"
                  >
                    &larr; {t('previous') || 'Previous'}
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
                    {t('next') || 'Next'} &rarr;
                  </Link>
                )}
              </div>
            )}
          </div>
        </main>

        {/* Side Panel */}
        {isPanelOpen && (
          <aside className="fixed inset-0 md:inset-auto md:top-[73px] md:right-0 md:bottom-0 md:w-[450px] bg-white dark:bg-boogle-dark border-l border-gray-200 dark:border-white/5 shadow-2xl z-50 md:z-20 overflow-hidden flex flex-col md:animate-slide-in-right animate-fade-in">
            {fetcher.data ? (
              <BookDetailsPanel
                book={fetcher.data}
                isLoading={fetcher.state === "loading"}
                onClose={closePanel}
              />
            ) : (
              <div className="h-full flex items-center justify-center">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-gray-900 dark:border-white"></div>
              </div>
            )}
          </aside>
        )}
      </div>

      {!isPanelOpen && <Footer />}
    </div>
  );
}
