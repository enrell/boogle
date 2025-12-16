import type { MetaFunction } from "react-router";
import { useSearchParams } from "react-router";
import { Link } from "react-router";
import { Header } from "../components/layout/Header";
import { Footer } from "../components/layout/Footer";
import { searchBooks, type SearchResult } from "../api";
import { useState, useEffect } from "react";

export const meta: MetaFunction = () => {
  return [
    { title: "Boogle Search" },
    { name: "description", content: "Search results for free books" },
  ];
};

export default function Search() {
  const [searchParams] = useSearchParams();
  const query = searchParams.get("q") || "";

  const [results, setResults] = useState<SearchResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [searchTime, setSearchTime] = useState<number>(0);

  useEffect(() => {
    if (!query.trim()) {
      setResults([]);
      return;
    }

    const fetchResults = async () => {
      setLoading(true);
      setError(null);
      const startTime = performance.now();

      try {
        const results = await searchBooks(query);
        setSearchTime(performance.now() - startTime);
        setResults(results);
      } catch (err) {
        setError((err as Error)?.message);
        setResults([]);
      } finally {
        setLoading(false);
      }
    };
    fetchResults();
  }, [query]);

  return (
    <div className="flex flex-col min-h-screen bg-white dark:bg-gray-900">
      <Header query={query} />

      <main className="flex-1 max-w-4xl w-full mx-auto px-4 py-6">
        {error && (
          <div className="bg-red-50 border border-red-200 rounded-lg p-4 mb-6">
            <p className="text-red-700 font-medium">{error}</p>
          </div>
        )}

        {!loading && !error && results.length > 0 && (
          <p className="text-gray-500 text-sm mb-6">
            About {results.length} results ({(searchTime / 1000).toFixed(2)} seconds)
          </p>
        )}

        {!loading && !error && results.length === 0 && query && (
          <div className="text-gray-600 dark:text-gray-400">
            <p className="text-lg mb-2">No results found for "{query}"</p>
            <p className="text-sm">Try different keywords or check your spelling.</p>
          </div>
        )}

        <div className="space-y-6">
          {loading ? (
            <div className="space-y-6">
              {[1, 2, 3].map((i) => (
                <div key={i} className="animate-pulse">
                  <div className="h-3 bg-gray-200 rounded w-64 mb-2"></div>
                  <div className="h-5 bg-gray-200 rounded w-96 mb-2"></div>
                  <div className="h-3 bg-gray-200 rounded w-80"></div>
                </div>
              ))}
            </div>
          ) : (
            results.map((book) => (
              <article key={book.book_id} className="group">
                <div className="text-sm text-gray-600 dark:text-gray-400 mb-1">
                  {book.url}
                </div>
                <Link
                  to={`/book/${book.book_id}`}
                  className="text-xl text-blue-700 dark:text-blue-400 hover:underline visited:text-purple-700 dark:visited:text-purple-400 font-medium block mb-1"
                >
                  {book.title || "Untitled"}
                </Link>
                <p className="text-sm text-gray-700 dark:text-gray-300">
                  <span className="text-gray-500">by</span>{" "}
                  <span className="font-medium">{book.author || "Unknown Author"}</span>
                </p>
              </article>
            ))
          )}
        </div>
      </main>
      <Footer />
    </div>
  );
}
