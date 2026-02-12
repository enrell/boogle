import { useState } from 'react';
import { IoClose, IoChevronBack } from 'react-icons/io5';

interface BookReaderProps {
  url: string;
  format: 'pdf' | 'epub' | 'html' | 'txt';
  title: string;
  onClose: () => void;
}

export function BookReader({ url, format, title, onClose }: BookReaderProps) {
  const [currentPage, setCurrentPage] = useState(1);
  const [totalPages, setTotalPages] = useState(0);

  const getEmbedUrl = () => {
    // For all formats, open directly (HTML works best in iframe)
    return url;
  };

  if (format === 'epub') {
    // For EPUB, we'll show a message with a download link since browser EPUB reading requires a library
    return (
      <div className="fixed inset-0 z-[60] bg-black/80 backdrop-blur-sm flex items-center justify-center p-4">
        <div className="bg-white dark:bg-gray-900 rounded-2xl max-w-lg w-full p-8 text-center">
          <button
            onClick={onClose}
            className="absolute top-4 right-4 p-2 hover:bg-gray-100 dark:hover:bg-white/10 rounded-full"
          >
            <IoClose className="w-6 h-6" />
          </button>
          
          <div className="mb-6">
            <div className="w-20 h-20 bg-blue-100 dark:bg-blue-900/30 rounded-full flex items-center justify-center mx-auto mb-4">
              <span className="text-2xl font-bold text-blue-600">EPUB</span>
            </div>
            <h3 className="text-xl font-bold mb-2">EPUB Reader</h3>
            <p className="text-gray-600 dark:text-gray-400">
              EPUB files require an e-reader app. You can download the file and open it with:
            </p>
          </div>
          
          <ul className="text-left text-sm text-gray-600 dark:text-gray-400 mb-6 space-y-2">
            <li>• Apple Books (iOS/macOS)</li>
            <li>• Google Play Books (Android)</li>
            <li>• Calibre (Desktop)</li>
            <li>• Any EPUB reader app</li>
          </ul>
          
          <div className="flex gap-3">
            <a
              href={url}
              download
              className="flex-1 px-6 py-3 bg-blue-600 text-white rounded-xl font-semibold hover:bg-blue-700 transition-colors"
            >
              Download EPUB
            </a>
            <button
              onClick={onClose}
              className="flex-1 px-6 py-3 border border-gray-300 dark:border-gray-600 rounded-xl font-medium hover:bg-gray-100 dark:hover:bg-white/5 transition-colors"
            >
              Cancel
            </button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="fixed inset-0 z-[60] bg-black/90 flex flex-col">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 bg-gray-900 text-white">
        <div className="flex items-center gap-3">
          <button
            onClick={onClose}
            className="p-2 hover:bg-white/10 rounded-lg transition-colors"
          >
            <IoChevronBack className="w-6 h-6" />
          </button>
          <h2 className="font-semibold truncate max-w-md">{title}</h2>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-xs text-gray-400 uppercase px-2 py-1 bg-white/10 rounded">
            {format}
          </span>
          <button
            onClick={onClose}
            className="p-2 hover:bg-white/10 rounded-lg transition-colors"
          >
            <IoClose className="w-6 h-6" />
          </button>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-hidden bg-white">
        <iframe
          src={getEmbedUrl()}
          className="w-full h-full"
          title={`Reader - ${title}`}
          sandbox="allow-same-origin allow-scripts"
        />
      </div>

      {/* Footer with download */}
      <div className="px-4 py-3 bg-gray-900 text-white flex items-center justify-between">
        <div className="text-sm text-gray-400">
          Reading mode
        </div>
        <a
          href={url}
          target="_blank"
          rel="noreferrer"
          className="text-sm text-blue-400 hover:text-blue-300"
        >
          Open in new tab →
        </a>
      </div>
    </div>
  );
}
