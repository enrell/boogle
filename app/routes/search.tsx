import type { MetaFunction, LoaderFunctionArgs } from "react-router";
import { useLoaderData, Link } from "react-router";
import { Header } from "../components/layout/Header";
import { Footer } from "../components/layout/Footer";
import { searchBooks, type SearchResult } from "../api.server";

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
};

export async function loader({ request }: LoaderFunctionArgs): Promise<LoaderData> {
  const url = new URL(request.url);
  const query = url.searchParams.get("q") || "";

  if (!query.trim()) {
    return { query, results: [], error: null, searchTime: 0 };
  }

  const startTime = Date.now();
  try {
    const results = await searchBooks(query);
    return {
      query,
      results,
      error: null,
      searchTime: Date.now() - startTime,
    };
  } catch (err) {
    return {
      query,
      results: [],
      error: (err as Error).message,
      searchTime: Date.now() - startTime,
    };
  }
}

export default function Search() {
  const { query, results, error, searchTime } = useLoaderData<typeof loader>();

  return (
    <div className="flex flex-col min-h-screen bg-white dark:bg-boogle-dark">
      <Header query={query} />

      <main className="flex-1 max-w-4xl w-full mx-auto px-4 py-6">
        {error && (
          <div className="bg-red-50 border border-red-200 rounded-lg p-4 mb-6">
            <p className="text-red-700 font-medium">{error}</p>
          </div>
        )}

        {!error && results.length > 0 && (
          <p className="text-gray-500 text-sm mb-6">
            About {results.length} results ({(searchTime / 1000).toFixed(2)} seconds)
          </p>
        )}

        {!error && results.length === 0 && query && (
          <div className="text-gray-600 dark:text-gray-400">
            <p className="text-lg mb-2">No results found for "{query}"</p>
            <p className="text-sm">Try different keywords or check your spelling.</p>
          </div>
        )}

        <div className="space-y-6">
          {results.map((book) => (
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
          ))}
        </div>
      </main>
      <Footer />
    </div>
  );
}
