import { useEffect } from "react";
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

  // Get files from primary source
  const files = book.primary_source?.files || [];
  const primaryUrl = book.primary_source?.url || "#";
  const sourceProvider = book.primary_source?.provider || "Unknown";

  // Use cover_url if available, otherwise find cover image in files
  const coverImage = book.cover_url || files.find(f =>
    f.format.toLowerCase().includes("image") ||
    f.url.toLowerCase().endsWith(".jpg") ||
    f.url.toLowerCase().endsWith(".png") ||
    f.url.toLowerCase().includes("cover")
  )?.url;

  // Find the "Read now!" or HTML/TXT link for reading
  const readNowFile = files.find(f => 
    f.format.toLowerCase().includes("html") ||
    f.format.toLowerCase().includes("txt") ||
    f.format.toLowerCase().includes("read")
  );

  // Get downloadable files (exclude images)
  const downloadFiles = files.filter(f =>
    !f.format.toLowerCase().includes("image") &&
    !f.url.toLowerCase().endsWith(".jpg") &&
    !f.url.toLowerCase().endsWith(".png")
  );

  // Build author string from authors array
  const authorString = book.authors?.length
    ? book.authors.map(a => a.name).join(", ")
    : t('unknown_author');

  return (
    <div className="flex flex-col h-full bg-white dark:bg-boogle-dark border-l border-gray-200 dark:border-white/5 shadow-xl">
      {/* Header Actions */}
      <div className="flex items-center justify-between p-4 border-b border-gray-200 dark:border-white/5 sticky top-0 bg-white dark:bg-boogle-dark z-10">
        <div className="flex gap-2">
          <button className="p-2 hover:bg-gray-100 dark:hover:bg-white/5 rounded-full text-gray-600 dark:text-gray-400 cursor-pointer" title="Share">
            <IoShareSocialOutline className="w-5 h-5" />
          </button>
        </div>
        <button
          onClick={onClose}
          className="p-2 hover:bg-gray-100 dark:hover:bg-white/5 rounded-full text-gray-500 dark:text-gray-400 cursor-pointer"
        >
          <IoClose className="w-6 h-6" />
        </button>
      </div>

      <div id="book-panel-content" className="flex-1 overflow-y-auto custom-scrollbar">
        <div className="p-6 space-y-8">
          {/* Header Info */}
          <div className="space-y-4">
            <h1 className="text-2xl font-serif font-medium text-gray-900 dark:text-gray-100 leading-tight">
              {book.title}
            </h1>
            <div className="text-sm text-gray-600 dark:text-gray-400">
              {t('by')} <span className="text-blue-600 dark:text-blue-400 cursor-pointer">{authorString}</span>
              {book.publication_year && <span> · {book.publication_year}</span>}
            </div>

            <div className="flex gap-3 pt-2">
              <a
                href={readNowFile?.url || primaryUrl}
                target="_blank"
                rel="noreferrer"
                className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-full text-sm font-medium hover:bg-blue-700 transition-colors"
              >
                <IoBookOutline /> {t('read_now')}
              </a>
              <button className="flex items-center gap-2 px-4 py-2 border border-gray-300 dark:border-gray-600 rounded-full text-sm font-medium hover:bg-gray-50 dark:hover:bg-white/5 dark:text-gray-300 transition-colors cursor-pointer">
                <IoBookmarkOutline /> {t('save')}
              </button>
            </div>
          </div>

          {/* Cover & Description */}
          <div className="flex flex-col gap-6">
            {coverImage ? (
              <div className="w-full flex justify-center bg-gray-50 dark:bg-white/5 p-8 rounded-xl">
                <CachedImage
                  src={coverImage}
                  alt={book.title || "Book cover"}
                  className="shadow-lg max-w-[180px] w-full object-cover aspect-[2/3] rounded"
                />
              </div>
            ) : (
              <div className="w-full flex justify-center bg-gray-50 dark:bg-white/5 p-8 rounded-xl">
                <div className="shadow-lg max-w-[180px] w-full aspect-[2/3] rounded bg-gray-200 dark:bg-white/10 flex items-center justify-center text-center p-4">
                  <span className="text-gray-400 dark:text-gray-500 font-medium text-sm">{t('no_preview')}</span>
                </div>
              </div>
            )}

            <div className="text-gray-700 dark:text-gray-300 text-sm leading-relaxed space-y-4">
              <p>
                This edition of <strong>{book.title}</strong> is available for free download in multiple formats.
                It is sourced from {sourceProvider.charAt(0).toUpperCase() + sourceProvider.slice(1)}.
              </p>
              {book.description && (
                <p>{book.description}</p>
              )}
            </div>
          </div>

          <hr className="border-gray-200 dark:border-white/5" />

          {/* About this edition table */}
          <div>
            <h3 className="text-lg font-medium text-gray-900 dark:text-gray-100 mb-4">{t('about_this_edition')}</h3>
            <div className="grid grid-cols-1 gap-y-3 text-sm">
              <div className="grid grid-cols-3">
                <span className="text-gray-500 dark:text-gray-500">{t('language')}</span>
                <span className="col-span-2 text-gray-900 dark:text-gray-300">{book.language || t('unknown')}</span>
              </div>
              <div className="grid grid-cols-3">
                <span className="text-gray-500 dark:text-gray-500">{t('subjects')}</span>
                <span className="col-span-2 text-gray-900 dark:text-gray-300">
                  {book.subjects?.join(", ") || t('unknown')}
                </span>
              </div>
              <div className="grid grid-cols-3">
                <span className="text-gray-500 dark:text-gray-500">{t('sources')}</span>
                <span className="col-span-2 text-gray-900 dark:text-gray-300">{book.source_count} {t('sources')}</span>
              </div>
              <div className="grid grid-cols-3">
                <span className="text-gray-500 dark:text-gray-500">{t('completeness')}</span>
                <span className="col-span-2 text-gray-900 dark:text-gray-300">
                  {(book.metadata_completeness * 100).toFixed(0)}%
                </span>
              </div>
            </div>
          </div>

          <hr className="border-gray-200 dark:border-white/5" />

          {/* Downloads */}
          <div>
            <h3 className="text-lg font-medium text-gray-900 dark:text-gray-100 mb-4">{t('available_formats')}</h3>
            <div className="flex flex-col gap-2">
              {downloadFiles.length > 0 ? downloadFiles.map((file, idx) => (
                <a
                  key={idx}
                  href={file.url}
                  target="_blank"
                  rel="noreferrer"
                  className="flex items-center gap-3 px-4 py-3 bg-gray-50 dark:bg-white/5 text-gray-700 dark:text-gray-300 rounded-lg hover:bg-gray-100 dark:hover:bg-white/10 transition-colors group"
                >
                  <IoDownloadOutline className="w-5 h-5 text-gray-400 group-hover:text-blue-500 transition-colors" />
                  <span className="text-sm font-medium">{file.format.toUpperCase()}</span>
                </a>
              )) : (
                <p className="text-sm text-gray-500">{t('no_download_formats')}</p>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
