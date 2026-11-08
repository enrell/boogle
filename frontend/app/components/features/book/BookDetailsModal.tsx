import { useEffect, useRef, useState } from "react";
import { IoClose } from "react-icons/io5";
import { BookDetailsPanel } from "./BookDetailsPanel";
import type { BookMetadata } from "~/types";

interface BookDetailsModalProps {
  book: BookMetadata;
  isLoading?: boolean;
  onClose: () => void;
}

export function BookDetailsModal({ book, isLoading, onClose }: BookDetailsModalProps) {
  const modalRef = useRef<HTMLDivElement>(null);
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
    
    // Close on Escape key
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        onClose();
      }
    };

    document.addEventListener("keydown", handleEscape);
    
    // Lock body scroll when modal is open
    const originalStyle = document.body.style.overflow;
    document.body.style.overflow = "hidden";

    return () => {
      document.removeEventListener("keydown", handleEscape);
      document.body.style.overflow = originalStyle;
      setMounted(false);
    };
  }, [onClose]);

  // Close when clicking on backdrop
  const handleBackdropClick = (e: React.MouseEvent) => {
    if (e.target === modalRef.current) {
      onClose();
    }
  };

  return (
    <div
      ref={modalRef}
      onClick={handleBackdropClick}
      className={`fixed inset-0 z-50 overflow-y-auto transition-opacity duration-300 ${
        mounted ? 'opacity-100' : 'opacity-0'
      }`}
    >
      {/* Backdrop with blur */}
      <div className="fixed inset-0 bg-black/60 backdrop-blur-sm transition-opacity" />
      
      {/* Modal container - centered */}
      <div className="flex min-h-full items-center justify-center p-4 sm:p-6 lg:p-8">
        <div
          className={`relative w-full max-w-5xl max-h-[95vh] bg-white dark:bg-boogle-dark rounded-3xl shadow-2xl overflow-hidden transition-all duration-300 ${
            mounted ? 'scale-100 opacity-100' : 'scale-95 opacity-0'
          }`}
          onClick={(e) => e.stopPropagation()}
        >
          {/* Close button - fixed position */}
          <button
            onClick={onClose}
            className="absolute top-4 right-4 z-50 p-3 rounded-full bg-black/20 hover:bg-black/40 text-white backdrop-blur-md transition-all duration-200 hover:scale-110"
          >
            <IoClose className="w-6 h-6" />
          </button>

          {/* Modal content */}
          {isLoading ? (
            <div className="h-[70vh] flex items-center justify-center">
              <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-gray-900 dark:border-white"></div>
            </div>
          ) : (
            <BookDetailsPanel
              book={book}
              isLoading={isLoading}
              onClose={onClose}
            />
          )}
        </div>
      </div>
    </div>
  );
}
