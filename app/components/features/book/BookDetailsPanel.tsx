import { useEffect, useState } from "react";
import { Link } from "react-router";
import { useTranslation } from "react-i18next";
import type { BookMetadata } from "~/types";
import { IoClose, IoBookOutline, IoDownloadOutline, IoBookmarkOutline, IoShareSocialOutline } from "react-icons/io5";
import { CachedImage } from "~/components/ui/CachedImage";

interface BookDetailsPanelProps {
  book: BookMetadata;
  isLoading?: boolean;
  onClose: () => void;
}

export function BookDetailsPanel({ book, isLoading, onClose }: BookDetailsPanelProps) {
  const { t } = useTranslation();
  const [showPreview, setShowPreview] = useState(false);

  useEffect(() => {
    const panel = document.getElementById("book-panel-content");
    if (panel) panel.scrollTop = 0;
  }, [book.canonical_id]);

  if (isLoading) {
    return (
      <div className="h-full w-full flex items-center justify-center">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-gray-900 dark:border-white"></div>
      </div>
    );
  }

  // Get files from ALL sources, not just primary
  const allFiles = [
    ...(book.primary_source?.files || []),
    ...(book.all_sources?.flatMap(s => s.files || []) || [])
  ];
  
  // Remove duplicates based on URL
  const uniqueFiles = allFiles.filter((file, index, self) =>
    index === self.findIndex(f => f.url === file.url)
  );
  
  const primaryUrl = book.primary_source?.url || "#";
  const sourceProvider = book.primary_source?.provider || "Unknown";

  // Use cover_url if available, otherwise find cover image in files
  const coverImage = book.cover_url || book.thumbnail_url || uniqueFiles.find(f =>
    f.format.toLowerCase().includes("image") ||
    f.url.toLowerCase().endsWith(".jpg") ||
    f.url.toLowerCase().endsWith(".png") ||
    f.url.toLowerCase().includes("cover")
  )?.url;

  // Find the "Read now!" or HTML/TXT link for reading
  const readNowFile = uniqueFiles.find(f =>
    f.format.toLowerCase().includes("html") ||
    f.format.toLowerCase().includes("txt") ||
    f.format.toLowerCase().includes("read")
  );

  // Find PDF for preview
  const pdfFile = uniqueFiles.find(f =>
    f.format.toLowerCase().includes("pdf")
  );

  // Get downloadable files - show all unique formats
  const downloadFiles = uniqueFiles.filter(f => {
    // Include if it has a URL and format
    if (!f.url || !f.format) return false;
    
    // Exclude image files
    const isImage = 
      f.format.toLowerCase().includes("image") ||
      f.url.toLowerCase().endsWith(".jpg") ||
      f.url.toLowerCase().endsWith(".png") ||
      f.url.toLowerCase().endsWith(".jpeg") ||
      f.url.toLowerCase().endsWith(".gif") ||
      f.url.toLowerCase().endsWith(".webp");
    
    return !isImage;
  });

  // Build author string from authors array
  const authorString = book.authors?.length
    ? book.authors.map(a => a.name).join(", ")
    : t('unknown_author');

  // Extract ISBN for OpenLibrary cover
  const isbn = book.isbn_13 || book.isbn_10;
  const openLibraryCover = isbn 
    ? `https://covers.openlibrary.org/b/isbn/${isbn}-L.jpg?default=false`
    : coverImage;

  return (
    <div className="flex flex-col h-full bg-white dark:bg-boogle-dark shadow-xl">
      {/* Header Actions */}
      <div className="flex items-center justify-between p-4 border-b border-gray-200 dark:border-white/5 sticky top-0 bg-white dark:bg-boogle-dark z-10">
        <div className="flex items-center gap-3">
          <span className="text-sm text-gray-500 dark:text-gray-400">
            {sourceProvider.charAt(0).toUpperCase() + sourceProvider.slice(1)}
          </span>
        </div>
        <div className="flex items-center gap-2">
          <button className="p-2 hover:bg-gray-100 dark:hover:bg-white/5 rounded-full text-gray-600 dark:text-gray-400 cursor-pointer" title="Share">
            <IoShareSocialOutline className="w-5 h-5" />
          </button>
          <button
            onClick={onClose}
            className="p-2 hover:bg-gray-100 dark:hover:bg-white/5 rounded-full text-gray-500 dark:text-gray-400 cursor-pointer"
          >
            <IoClose className="w-6 h-6" />
          </button>
        </div>
      </div>

      <div id="book-panel-content" className="flex-1 overflow-y-auto custom-scrollbar">
        <div className="p-6 space-y-6">
          {/* Large Cover Preview */}
          <div className="flex flex-col md:flex-row gap-6">
            {/* Cover Image - Larger */}
            <div className="w-full md:w-1/3 flex-shrink-0">
              <div className="relative aspect-[2/3] bg-gray-100 dark:bg-white/5 rounded-xl overflow-hidden shadow-lg">
                {openLibraryCover ? (
                  <CachedImage
                    src={openLibraryCover}
                    alt={book.title || "Book cover"}
                    className="w-full h-full object-cover"
                  />
                ) : (
                  <div className="w-full h-full flex flex-col items-center justify-center text-gray-400 dark:text-gray-600 p-6">
                    <svg className="w-20 h-20 mb-3 opacity-30" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M12 6.253v13m0-13C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253m0-13C13.168 5.477 14.754 5 16.5 5c1.747 0 3.332.477 4.5 1.253v13C19.832 18.477 18.247 18 16.5 18c-1.746 0-3.332.477-4.5 1.253" />
                    </svg>
                    <span className="text-sm opacity-70">{t('no_preview')}</span>
                  </div>
                )}
              </div>
            </div>

            {/* Book Info */}
            <div className="flex-1 space-y-4">
              <div>
                <h1 className="text-2xl font-bold text-gray-900 dark:text-white leading-tight mb-2">
                  {book.title}
                </h1>
                <div className="text-base text-gray-600 dark:text-gray-400">
                  {t('by')} <span className="text-blue-600 dark:text-blue-400 font-medium">{authorString}</span>
                  {book.publication_year && <span className="text-gray-400"> · {book.publication_year}</span>}
                </div>
              </div>

              {/* Action Buttons */}
              <div className="flex flex-wrap gap-3">
                <a
                  href={readNowFile?.url || primaryUrl}
                  target="_blank"
                  rel="noreferrer"
                  className="flex items-center gap-2 px-6 py-3 bg-blue-600 text-white rounded-full text-sm font-medium hover:bg-blue-700 transition-colors shadow-md hover:shadow-lg"
                >
                  <IoBookOutline className="w-5 h-5" />
                  {t('read_now')}
                </a>
                {pdfFile && (
                  <button 
                    onClick={() => setShowPreview(!showPreview)}
                    className="flex items-center gap-2 px-6 py-3 border border-gray-300 dark:border-gray-600 rounded-full text-sm font-medium hover:bg-gray-50 dark:hover:bg-white/5 dark:text-gray-300 transition-colors"
                  >
                    {showPreview ? 'Hide Preview' : 'Preview'}
                  </button>
                )}
                <button className="p-3 border border-gray-300 dark:border-gray-600 rounded-full hover:bg-gray-50 dark:hover:bg-white/5 transition-colors">
                  <IoBookmarkOutline className="w-5 h-5 text-gray-600 dark:text-gray-400" />
                </button>
              </div>

              {/* Quick Info */}
              <div className="flex flex-wrap gap-4 text-sm text-gray-500 dark:text-gray-500">
                {book.language && (
                  <span className="flex items-center gap-1">
                    <span className="uppercase font-medium">{book.language}</span>
                  </span>
                )}
                {book.page_count && (
                  <span>{book.page_count} pages</span>
                )}
                {book.metadata_completeness > 0 && (
                  <span>{(book.metadata_completeness * 100).toFixed(0)}% complete</span>
                )}
              </div>
            </div>
          </div>

          {/* PDF Preview */}
          {showPreview && pdfFile && (
            <div className="border border-gray-200 dark:border-white/10 rounded-xl overflow-hidden">
              <div className="bg-gray-100 dark:bg-white/5 px-4 py-2 border-b border-gray-200 dark:border-white/10 flex justify-between items-center">
                <span className="text-sm font-medium text-gray-700 dark:text-gray-300">PDF Preview</span>
                <button 
                  onClick={() => setShowPreview(false)}
                  className="text-gray-500 hover:text-gray-700"
                >
                  Close
                </button>
              </div>
              <div className="h-[500px] bg-gray-50 dark:bg-black/20">
                <iframe
                  src={`${pdfFile.url}#page=1&zoom=page-fit`}
                  className="w-full h-full"
                  title={`Preview of ${book.title}`}
                />
              </div>
            </div>
          )}

          {/* Description */}
          {book.description && (
            <div className="bg-gray-50 dark:bg-white/5 rounded-xl p-4">
              <h3 className="text-sm font-semibold text-gray-900 dark:text-white mb-2">Description</h3>
              <p className="text-sm text-gray-600 dark:text-gray-400 leading-relaxed">
                {book.description}
              </p>
            </div>
          )}

          <hr className="border-gray-200 dark:border-white/5" />

          {/* About this edition table */}
          <div>
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">{t('about_this_edition')}</h3>
            <div className="grid grid-cols-1 gap-y-3 text-sm">
              {book.language && (
                <div className="grid grid-cols-3 py-2 border-b border-gray-100 dark:border-white/5">
                  <span className="text-gray-500 dark:text-gray-500">{t('language')}</span>
                  <span className="col-span-2 text-gray-900 dark:text-gray-300">{book.language}</span>
                </div>
              )}
              {book.subjects && book.subjects.length > 0 && (
                <div className="grid grid-cols-3 py-2 border-b border-gray-100 dark:border-white/5">
                  <span className="text-gray-500 dark:text-gray-500">{t('subjects')}</span>
                  <span className="col-span-2 text-gray-900 dark:text-gray-300">
                    {book.subjects.slice(0, 5).join(", ")}
                  </span>
                </div>
              )}
              <div className="grid grid-cols-3 py-2 border-b border-gray-100 dark:border-white/5">
                <span className="text-gray-500 dark:text-gray-500">{t('sources')}</span>
                <span className="col-span-2 text-gray-900 dark:text-gray-300">{book.source_count} {t('sources')}</span>
              </div>
              <div className="grid grid-cols-3 py-2 border-b border-gray-100 dark:border-white/5">
                <span className="text-gray-500 dark:text-gray-500">{t('completeness')}</span>
                <span className="col-span-2 text-gray-900 dark:text-gray-300">
                  {(book.metadata_completeness * 100).toFixed(0)}%
                </span>
              </div>
              {isbn && (
                <div className="grid grid-cols-3 py-2 border-b border-gray-100 dark:border-white/5">
                  <span className="text-gray-500 dark:text-gray-500">ISBN</span>
                  <span className="col-span-2 text-gray-900 dark:text-gray-300 font-mono text-xs">{isbn}</span>
                </div>
              )}
            </div>
          </div>

          <hr className="border-gray-200 dark:border-white/5" />

          {/* Downloads */}
          <div>
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">{t('available_formats')}</h3>
            <div className="grid grid-cols-2 sm:grid-cols-3 gap-3">
              {downloadFiles.length > 0 ? downloadFiles.map((file, idx) => (
                <a
                  key={idx}
                  href={file.url}
                  target="_blank"
                  rel="noreferrer"
                  className="flex items-center justify-center gap-2 px-4 py-3 bg-gray-50 dark:bg-white/5 text-gray-700 dark:text-gray-300 rounded-xl hover:bg-gray-100 dark:hover:bg-white/10 transition-colors group border border-gray-200 dark:border-white/10 hover:border-blue-300 dark:hover:border-blue-500"
                >
                  <IoDownloadOutline className="w-4 h-4 text-gray-400 group-hover:text-blue-500 transition-colors" />
                  <span className="text-sm font-medium">{file.format.toUpperCase()}</span>
                </a>
              )) : (
                <p className="text-sm text-gray-500 col-span-full">{t('no_download_formats')}</p>
              )}
            </div>
          </div>

          {/* View on Source Link */}
          <div className="pt-4">
            <a
              href={primaryUrl}
              target="_blank"
              rel="noreferrer"
              className="text-sm text-blue-600 dark:text-blue-400 hover:underline"
            >
              View on {sourceProvider.charAt(0).toUpperCase() + sourceProvider.slice(1)} →
            </a>
          </div>
        </div>
      </div>
    </div>
  );
}
