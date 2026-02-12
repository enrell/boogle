import { useEffect, useState, useCallback } from "react";
import { useTranslation } from "react-i18next";
import type { BookMetadata } from "~/types";
import { 
  IoBookOutline, 
  IoDownloadOutline, 
  IoBookmarkOutline, 
  IoShareSocialOutline,
  IoLanguage,
  IoCalendarOutline,
  IoDocumentTextOutline,
  IoGlobeOutline,
  IoEyeOutline,
  IoReaderOutline
} from "react-icons/io5";
import { CachedImage } from "~/components/ui/CachedImage";
import { BookReader } from "./BookReader";

interface BookDetailsPanelProps {
  book: BookMetadata;
  isLoading?: boolean;
  onClose: () => void;
}

interface OpenLibraryData {
  description?: string;
  subjects?: string[];
  publish_date?: string;
  publishers?: string[];
  isbn_10?: string[];
  isbn_13?: string[];
  covers?: number[];
}

export function BookDetailsPanel({ book, isLoading, onClose }: BookDetailsPanelProps) {
  const { t } = useTranslation();
  const [showPreview, setShowPreview] = useState(false);
  const [openLibraryData, setOpenLibraryData] = useState<OpenLibraryData | null>(null);
  const [isFetchingOL, setIsFetchingOL] = useState(false);
  const [readerFile, setReaderFile] = useState<{ url: string; format: string } | null>(null);

  // Fetch OpenLibrary data
  const fetchOpenLibraryData = useCallback(async () => {
    const isbn = book.isbn_13 || book.isbn_10;
    if (!isbn) return;

    setIsFetchingOL(true);
    try {
      const response = await fetch(`https://openlibrary.org/isbn/${isbn}.json`);
      if (response.ok) {
        const data = await response.json();
        setOpenLibraryData(data);
      }
    } catch (e) {
      console.warn("Failed to fetch OpenLibrary data:", e);
    }
    setIsFetchingOL(false);
  }, [book.isbn_13, book.isbn_10]);

  useEffect(() => {
    fetchOpenLibraryData();
  }, [fetchOpenLibraryData]);

  // Get files from ALL sources
  const allFiles = [
    ...(book.primary_source?.files || []),
    ...(book.all_sources?.flatMap(s => s.files || []) || [])
  ];
  
  // Remove duplicates based on URL and filter out invalid entries
  const uniqueFiles = allFiles
    .filter(file => file && file.url) // Filter out null/undefined entries
    .filter((file, index, self) =>
      index === self.findIndex(f => f.url === file.url)
    );
  
  // For Gutenberg books, if no files are provided, generate them
  const { provider, bookId } = (() => {
    const parts = book.canonical_id.split(':');
    return { provider: parts[0], bookId: parts.slice(1).join(':') };
  })();
  
  const gutenbergFiles = provider === 'gutenberg' && allFiles.filter(f => f && f.url).length === 0
    ? [
        { format: 'html', url: `https://www.gutenberg.org/files/${bookId}/${bookId}-h/${bookId}-h.htm` },
        { format: 'txt', url: `https://www.gutenberg.org/files/${bookId}/${bookId}-0.txt` },
        { format: 'epub', url: `https://www.gutenberg.org/ebooks/${bookId}.epub` },
        { format: 'pdf', url: `https://www.gutenberg.org/ebooks/${bookId}.pdf` },
      ]
    : [];
  
  const displayFiles = [...uniqueFiles, ...gutenbergFiles];
  
  const primaryUrl = book.primary_source?.url || "#";
  const sourceProvider = book.primary_source?.provider || "Unknown";

  // Get cover from OpenLibrary or API
  const isbn = book.isbn_13 || book.isbn_10;
  const coverImage = book.cover_url || book.thumbnail_url || 
    (isbn ? `https://covers.openlibrary.org/b/isbn/${isbn}-L.jpg?default=false` : null) ||
    (openLibraryData?.covers?.[0] ? `https://covers.openlibrary.org/b/id/${openLibraryData.covers[0]}-L.jpg` : null);

  // Group files by format
  const filesByFormat = displayFiles.reduce((acc, file) => {
    const format = file.format?.toLowerCase() || 'unknown';
    if (!acc[format]) acc[format] = [];
    acc[format].push(file);
    return acc;
  }, {} as Record<string, typeof displayFiles>);

  // Common book formats to prioritize
  const formatPriority = ['pdf', 'epub', 'txt', 'html', 'mobi', 'azw3'];
  const sortedFormats = Object.keys(filesByFormat).sort((a, b) => {
    const indexA = formatPriority.indexOf(a);
    const indexB = formatPriority.indexOf(b);
    if (indexA === -1 && indexB === -1) return a.localeCompare(b);
    if (indexA === -1) return 1;
    if (indexB === -1) return -1;
    return indexA - indexB;
  });

  // Build author string
  const authorString = book.authors?.length 
    ? book.authors.map(a => a.name).join(", ")
    : t('unknown_author');

  // Get description from various sources
  const description = book.description || openLibraryData?.description || null;

  // Find readable files (PDF, EPUB, HTML, TXT)
  const readableFiles = displayFiles.filter(f => {
    const format = f.format?.toLowerCase();
    return format === 'pdf' || format === 'epub' || format === 'html' || format === 'txt';
  });

  const pdfFile = readableFiles.find(f => f.format?.toLowerCase() === 'pdf');
  const epubFile = readableFiles.find(f => f.format?.toLowerCase() === 'epub');
  const htmlFile = readableFiles.find(f => f.format?.toLowerCase() === 'html');
  const txtFile = readableFiles.find(f => f.format?.toLowerCase() === 'txt');
  
  // Best file for reading (priority: HTML, PDF, TXT, EPUB)
  const bestReadFile = htmlFile || pdfFile || txtFile || epubFile;

  return (
    <div className="flex flex-col h-full bg-white dark:bg-boogle-dark">
      {/* Main Content - Two Column Layout */}
      <div className="flex-1 overflow-y-auto">
        <div className="grid grid-cols-1 lg:grid-cols-[400px_1fr] min-h-full">
          
          {/* Left Column - Cover & Quick Actions */}
          <div className="bg-gray-50 dark:bg-white/5 p-6 lg:p-8 flex flex-col items-center border-b lg:border-b-0 lg:border-r border-gray-200 dark:border-white/10">
            {/* Large Cover */}
            <div className="relative w-full max-w-[300px] aspect-[2/3] rounded-2xl overflow-hidden shadow-2xl mb-6">
              {coverImage ? (
                <CachedImage
                  src={coverImage}
                  alt={book.title || "Book cover"}
                  className="w-full h-full object-cover"
                />
              ) : (
                <div className="w-full h-full flex flex-col items-center justify-center text-gray-400 dark:text-gray-600 bg-gray-200 dark:bg-white/10">
                  <IoBookOutline className="w-20 h-20 mb-4 opacity-30" />
                  <span className="text-sm opacity-70">{t('no_preview')}</span>
                </div>
              )}
              
              {/* Provider Badge */}
              <div className="absolute top-4 left-4">
                <span className="bg-black/60 text-white text-xs font-bold px-3 py-1.5 rounded-full backdrop-blur-sm uppercase tracking-wide">
                  {sourceProvider}
                </span>
              </div>
            </div>

            {/* Quick Actions */}
            <div className="w-full space-y-3">
              {bestReadFile ? (
                <button
                  onClick={() => setReaderFile({ url: bestReadFile.url, format: bestReadFile.format.toLowerCase() })}
                  className="flex items-center justify-center gap-2 w-full px-6 py-4 bg-blue-600 text-white rounded-xl font-semibold hover:bg-blue-700 transition-all duration-200 shadow-lg hover:shadow-xl"
                >
                  <IoReaderOutline className="w-5 h-5" />
                  Read {bestReadFile.format.toUpperCase()}
                </button>
              ) : (
                <a
                  href={primaryUrl}
                  target="_blank"
                  rel="noreferrer"
                  className="flex items-center justify-center gap-2 w-full px-6 py-4 bg-blue-600 text-white rounded-xl font-semibold hover:bg-blue-700 transition-all duration-200 shadow-lg hover:shadow-xl"
                >
                  <IoGlobeOutline className="w-5 h-5" />
                  View on {sourceProvider}
                </a>
              )}

              <div className="flex gap-3">
                <button className="flex-1 flex items-center justify-center gap-2 px-4 py-3 border border-gray-300 dark:border-gray-600 rounded-xl font-medium hover:bg-gray-100 dark:hover:bg-white/5 transition-colors">
                  <IoBookmarkOutline className="w-5 h-5" />
                  {t('save')}
                </button>
                <button className="flex-1 flex items-center justify-center gap-2 px-4 py-3 border border-gray-300 dark:border-gray-600 rounded-xl font-medium hover:bg-gray-100 dark:hover:bg-white/5 transition-colors">
                  <IoShareSocialOutline className="w-5 h-5" />
                  Share
                </button>
              </div>
              
              {/* Additional format buttons */}
              <div className="flex flex-wrap gap-2 pt-2">
                {pdfFile && bestReadFile?.url !== pdfFile.url && (
                  <button
                    onClick={() => setReaderFile({ url: pdfFile.url, format: 'pdf' })}
                    className="flex items-center gap-2 px-3 py-2 bg-red-50 dark:bg-red-900/20 text-red-700 dark:text-red-300 rounded-lg text-sm font-medium hover:bg-red-100 dark:hover:bg-red-900/30 transition-colors"
                  >
                    <IoReaderOutline className="w-4 h-4" />
                    PDF
                  </button>
                )}
                {epubFile && bestReadFile?.url !== epubFile.url && (
                  <button
                    onClick={() => setReaderFile({ url: epubFile.url, format: 'epub' })}
                    className="flex items-center gap-2 px-3 py-2 bg-purple-50 dark:bg-purple-900/20 text-purple-700 dark:text-purple-300 rounded-lg text-sm font-medium hover:bg-purple-100 dark:hover:bg-purple-900/30 transition-colors"
                  >
                    <IoReaderOutline className="w-4 h-4" />
                    EPUB
                  </button>
                )}
                {htmlFile && bestReadFile?.url !== htmlFile.url && (
                  <button
                    onClick={() => setReaderFile({ url: htmlFile.url, format: 'html' })}
                    className="flex items-center gap-2 px-3 py-2 bg-orange-50 dark:bg-orange-900/20 text-orange-700 dark:text-orange-300 rounded-lg text-sm font-medium hover:bg-orange-100 dark:hover:bg-orange-900/30 transition-colors"
                  >
                    <IoReaderOutline className="w-4 h-4" />
                    HTML
                  </button>
                )}
                {txtFile && bestReadFile?.url !== txtFile.url && (
                  <button
                    onClick={() => setReaderFile({ url: txtFile.url, format: 'txt' })}
                    className="flex items-center gap-2 px-3 py-2 bg-gray-100 dark:bg-white/10 text-gray-700 dark:text-gray-300 rounded-lg text-sm font-medium hover:bg-gray-200 dark:hover:bg-white/20 transition-colors"
                  >
                    <IoReaderOutline className="w-4 h-4" />
                    TXT
                  </button>
                )}
              </div>
            </div>
          </div>

          {/* Right Column - Details */}
          <div className="p-6 lg:p-8 space-y-8">
            {/* Header */}
            <div>
              <h1 className="text-3xl lg:text-4xl font-bold text-gray-900 dark:text-white leading-tight mb-3">
                {book.title}
              </h1>
              {book.subtitle && (
                <p className="text-xl text-gray-600 dark:text-gray-400 mb-3">
                  {book.subtitle}
                </p>
              )}
              <div className="flex flex-wrap items-center gap-x-4 gap-y-2 text-gray-600 dark:text-gray-400">
                <span className="flex items-center gap-1.5">
                  {t('by')} <span className="text-blue-600 dark:text-blue-400 font-medium">{authorString}</span>
                </span>
                {book.publication_year && (
                  <span className="flex items-center gap-1.5">
                    <IoCalendarOutline className="w-4 h-4" />
                    {book.publication_year}
                  </span>
                )}
                {book.language && (
                  <span className="flex items-center gap-1.5 uppercase font-medium">
                    <IoLanguage className="w-4 h-4" />
                    {book.language}
                  </span>
                )}
              </div>
            </div>

            {/* Metadata Grid */}
            <div className="grid grid-cols-2 sm:grid-cols-3 gap-4">
              {book.page_count && (
                <div className="bg-gray-50 dark:bg-white/5 rounded-xl p-4">
                  <div className="text-xs text-gray-500 dark:text-gray-400 uppercase font-medium mb-1">Pages</div>
                  <div className="text-2xl font-bold text-gray-900 dark:text-white flex items-center gap-2">
                    <IoDocumentTextOutline className="w-5 h-5 text-blue-500" />
                    {book.page_count}
                  </div>
                </div>
              )}
              {book.language && (
                <div className="bg-gray-50 dark:bg-white/5 rounded-xl p-4">
                  <div className="text-xs text-gray-500 dark:text-gray-400 uppercase font-medium mb-1">Language</div>
                  <div className="text-2xl font-bold text-gray-900 dark:text-white flex items-center gap-2">
                    <IoLanguage className="w-5 h-5 text-green-500" />
                    {book.language.toUpperCase()}
                  </div>
                </div>
              )}
              {isbn && (
                <div className="bg-gray-50 dark:bg-white/5 rounded-xl p-4">
                  <div className="text-xs text-gray-500 dark:text-gray-400 uppercase font-medium mb-1">ISBN</div>
                  <div className="text-sm font-mono text-gray-700 dark:text-gray-300 truncate" title={isbn}>
                    {isbn}
                  </div>
                </div>
              )}
            </div>

            {/* Description */}
            {description && (
              <div className="bg-blue-50 dark:bg-blue-900/20 rounded-2xl p-6">
                <h3 className="text-lg font-bold text-gray-900 dark:text-white mb-3 flex items-center gap-2">
                  <IoEyeOutline className="w-5 h-5 text-blue-500" />
                  About this book
                </h3>
                <p className="text-gray-700 dark:text-gray-300 leading-relaxed">
                  {typeof description === 'string' 
                    ? description 
                    : (description as { value?: string })?.value || ''}
                </p>
              </div>
            )}

            {/* Subjects */}
            {(book.subjects || openLibraryData?.subjects) && (
              <div>
                <h3 className="text-lg font-bold text-gray-900 dark:text-white mb-3">Subjects</h3>
                <div className="flex flex-wrap gap-2">
                  {(book.subjects || openLibraryData?.subjects || []).map((subject, idx) => (
                    <span 
                      key={idx}
                      className="px-3 py-1.5 bg-gray-100 dark:bg-white/10 text-gray-700 dark:text-gray-300 rounded-full text-sm hover:bg-gray-200 dark:hover:bg-white/20 transition-colors cursor-pointer"
                    >
                      {subject}
                    </span>
                  ))}
                </div>
              </div>
            )}

            {/* Available Formats */}
            <div>
              <h3 className="text-lg font-bold text-gray-900 dark:text-white mb-4 flex items-center gap-2">
                <IoDownloadOutline className="w-5 h-5 text-green-500" />
                {t('available_formats')}
              </h3>
              
              {sortedFormats.length > 0 ? (
                <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-3">
                  {sortedFormats.flatMap((format) => {
                    const files = filesByFormat[format];
                    return files.map((file, idx) => (
                      <a
                        key={`${format}-${idx}`}
                        href={file.url}
                        target="_blank"
                        rel="noreferrer"
                        className="flex items-center gap-3 px-4 py-4 bg-gray-50 dark:bg-white/5 border border-gray-200 dark:border-white/10 rounded-xl hover:bg-gray-100 dark:hover:bg-white/10 hover:border-blue-300 dark:hover:border-blue-500 transition-all group"
                      >
                        <div className="w-10 h-10 rounded-lg bg-blue-100 dark:bg-blue-900/30 flex items-center justify-center">
                          <IoDownloadOutline className="w-5 h-5 text-blue-600 dark:text-blue-400" />
                        </div>
                        <div>
                          <div className="font-semibold text-gray-900 dark:text-white uppercase">
                            {format}
                          </div>
                          <div className="text-xs text-gray-500">Download</div>
                        </div>
                      </a>
                    ));
                  })}
                </div>
              ) : (
                <div className="bg-gray-50 dark:bg-white/5 rounded-xl p-6 text-center">
                  <IoDownloadOutline className="w-12 h-12 text-gray-400 mx-auto mb-3" />
                  <p className="text-gray-500 dark:text-gray-400">{t('no_download_formats')}</p>
                  <p className="text-sm text-gray-400 mt-2">
                    This book may be available directly on {sourceProvider}
                  </p>
                  <a
                    href={primaryUrl}
                    target="_blank"
                    rel="noreferrer"
                    className="inline-flex items-center gap-2 mt-4 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
                  >
                    <IoGlobeOutline className="w-4 h-4" />
                    View on {sourceProvider}
                  </a>
                </div>
              )}
            </div>

            {/* View on Source */}
            <div className="pt-4 border-t border-gray-200 dark:border-white/10">
              <a
                href={primaryUrl}
                target="_blank"
                rel="noreferrer"
                className="inline-flex items-center gap-2 text-blue-600 dark:text-blue-400 font-medium hover:underline"
              >
                <IoGlobeOutline className="w-4 h-4" />
                View on {sourceProvider.charAt(0).toUpperCase() + sourceProvider.slice(1)} →
              </a>
            </div>
          </div>
      </div>
  </div>
    
    {/* Book Reader Modal */}
    {readerFile ? (
      <BookReader
        url={readerFile.url}
        format={readerFile.format as 'pdf' | 'epub' | 'html' | 'txt'}
        title={book.title}
        onClose={() => setReaderFile(null)}
      />
    ) : null}
  </div>
  );
}
