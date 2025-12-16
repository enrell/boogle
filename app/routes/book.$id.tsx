import { useLoaderData } from "react-router";
import type { MetaFunction, LoaderFunctionArgs } from "react-router";
import { Footer } from "../components/layout/Footer";
import { Header } from "../components/layout/Header";
import { Logo } from "~/components/ui/Logo";
import { getBookMetadata } from "~/api.server";
import { type BookMetadata } from "~/types";
import { Link } from "react-router";

type LoaderData = {
    book: BookMetadata;
};

export async function loader({ params }: LoaderFunctionArgs): Promise<LoaderData> {
    const bookId = params.id!;
    const bookData = await getBookMetadata(bookId);
    return { book: bookData };
}

export const meta: MetaFunction = ({ data }) => {
    const loaderData = data as LoaderData | undefined;

    const title = loaderData?.book?.title
        ? `${loaderData.book.title} - Boogle` : "Book Details - Boogle";

    const description = loaderData?.book?.title
        ? `Details for ${loaderData.book.title}` : "Book details";

    return [
        { title },
        { name: "description", content: description },
    ];
};

export function ErrorBoundary({ error }: { error: Error }) {
    return (
        <div className="flex flex-col items-center min-h-screen pt-20">
            <Logo />
            <main className="flex-1 max-w-6xl mx-auto py-10">
                <p className="text-md text-red-800"> {error.message} </p>
            </main>
            <Footer />
        </div>
    );
}

export default function BookDetail() {
    const { book } = useLoaderData<typeof loader>();
    const uniqueFiles = book.files.filter((item, index, self) =>
        index === self.findIndex(f => f.format === item.format))
    const coverImage = book.cover_url || book.files.find(f => f.format.includes("image/jpeg") || f.format.includes("image/png") || f.url.endsWith(".jpg"))?.url;

    return (
        <div className="flex flex-col min-h-screen bg-white dark:bg-boogle-dark">
            <Header />
            <main className="flex-1 w-full max-w-6xl mx-auto p-5">
                <div className="mb-6">
                    <Link to="/">
                        <button className="flex items-center gap-2 text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200 transition-colors">
                            <span>←</span> Back to Search
                        </button>
                    </Link>
                </div>

                <div className="flex flex-col md:flex-row gap-10 bg-white dark:bg-boogle-dark-lighter rounded-xl shadow-sm border border-gray-200 dark:border-white/5 p-8">

                    {/* Cover Image Section */}
                    <div className="w-full md:w-1/3 flex justify-center md:justify-start">
                        {coverImage ? (
                            <img
                                src={coverImage}
                                alt={`Cover of ${book.title}`}
                                className="rounded-lg shadow-md w-full max-w-[300px] object-cover aspect-[2/3]"
                            />
                        ) : (
                            <div className="w-full max-w-[300px] aspect-[2/3] bg-gray-100 dark:bg-white/5 rounded-lg flex items-center justify-center text-gray-400 dark:text-gray-600 border border-gray-200 dark:border-white/10">
                                <span className="text-lg font-medium">No Cover</span>
                            </div>
                        )}
                    </div>

                    <div className="flex-1 flex flex-col gap-6">
                        {book.title ? (
                            <h1 className="text-3xl text-center font-bold text-gray-900 dark:text-white">{book.title}</h1>
                        ) : (
                            <h1 className="text-3xl font-bold text-gray-900 dark:text-white"> Not exist title </h1>
                        )}
                        {book.author ? (
                            <p> <span className="font-bold"> Author: </span>{book.author} </p>
                        ) : (
                            <p> <span className="font-bold"> Author: </span>Not exist </p>
                        )}
                        {book.release_date ? (
                            <p> <span className="font-bold"> Release data: </span> {book.release_date}</p>
                        ) : (
                            <p> <span className="font-bold"> Release data: </span> Not exist</p>
                        )}
                        {book.downloads ? (
                            <p> <span className="font-bold"> Downloads: </span> {book.downloads}</p>
                        ) : (
                            <p> <span className="font-bold"> Downloads: </span> Not exist</p>
                        )}
                        {book.original_publication ? (
                            <p> <span className="font-bold"> Original_publication: </span> {book.original_publication}</p>
                        ) : (
                            <p> <span className="font-bold"> Original_publication: </span> Not exist</p>
                        )}
                        {book.illustrator ? (
                            <p> <span className="font-bold"> Illustrator: </span> {book.illustrator}</p>
                        ) : (
                            <p> <span className="font-bold">Illustrator: </span> Not exist</p>
                        )}
                        {book.copyright_status ? (
                            <p> <span className="font-bold"> Copyright: </span> {book.copyright_status}</p>
                        ) : (
                            <p> <span className="font-bold"> Copyright: </span> Not exist</p>
                        )}
                        {book.credits ? (
                            <p className="max-w-sm"> <span className="font-bold"> Credits: </span> {book.credits}</p>
                        ) : (
                            <p> <span className="font-bold"> Credits: </span> Not exist</p>
                        )}
                        <a
                            href={book.url}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600 text-sm font-medium max-w-52 text-center">
                            View on Gutenberg
                        </a>

                        <h2 className="font-bold text-2xl pt-5 text-center"> Publication details </h2>
                        <p> <span className="font-bold"> Book </span> ID:{book.book_id}</p>
                        {book.language ? (
                            <p> <span className="font-bold"> Language: </span> {book.language}</p>
                        ) : (
                            <p> <span className="font-bold"> Language: </span> No lenguage </p>
                        )}
                        {book.category ? (
                            <p> <span className="font-bold"> Category: </span> {book.category}</p>
                        ) : (
                            <p> <span className="font-bold"> Category: </span> No category </p>
                        )}

                        {book.files && book.files.length > 0 ? (
                            <div className="flex flex-col items-center">
                                <h1 className="font-bold text-2xl pt-5"> Formatos disponíveis  </h1>
                                {uniqueFiles.map((file, index) => (
                                    <a key={index} href={file.url} target="_blank" rel="noopener noreferrer">
                                        <span className="flex flex-col cursor-pointer px-4 py-2 bg-green-700 text-white rounded text-sm font-medium max-w-52 text-center m-6">
                                            {file.format.toUpperCase()}
                                        </span>
                                    </a>
                                ))}
                            </div>
                        ) : (
                            <h1> There are no formats available for downloads </h1>
                        )}
                    </div>
                </div>




            </main>
            <Footer />
        </div>
    );
}
