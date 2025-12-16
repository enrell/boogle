export interface BookMetadata {
    source: string;
    book_id: string;
    url: string;
    title: string | null;
    author: string | null;
    illustrator: string | null;
    release_date: string | null;
    language: string | null;
    category: string | null;
    original_publication: string | null;
    credits: string | null;
    copyright_status: string | null;
    downloads: string | null;
    cover_url: string | null;
    files: Array<{ format: string; url: string }>;
}

export interface SearchResult {
    book_id: string;
    title: string;
    author: string;
    score: number;
    url: string;
}
