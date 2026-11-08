import type { LoaderFunctionArgs } from "react-router";
import { getBookMetadata } from "~/api.server";

export async function loader({ params }: LoaderFunctionArgs) {
    const bookId = params.id;
    if (!bookId) throw new Error("Book ID is required");

    const book = await getBookMetadata(bookId);
    return book;
}
