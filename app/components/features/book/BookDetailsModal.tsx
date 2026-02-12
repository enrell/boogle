import { useEffect, useRef } from "react";
import { BookDetailsPanel } from "./BookDetailsPanel";
import type { BookMetadata } from "~/types";

interface BookDetailsModalProps {
  book: BookMetadata;
  isLoading?: boolean;
  onClose: () => void;
}

export function BookDetailsModal({ book, isLoading, onClose }: BookDetailsModalProps) {
  const modalRef = useRef<HTMLDivElement>(null);

  // Close on Escape key
  useEffect(() => {
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        onClose();
      }
    };

    document.addEventListener("keydown", handleEscape);
    return () => document.removeEventListener("keydown", handleEscape);
  }, [onClose]);

  // Lock body scroll when modal is open
  useEffect(() => {
    const originalStyle = window.getComputedStyle(document.body).overflow;
    document.body.style.overflow = "hidden";
    
    return () => {
      document.body.style.overflow = originalStyle;
    };
  }, []);

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
      className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/50 backdrop-blur-sm animate-fade-in"
    >
      <div 
        className="relative w-full max-w-2xl max-h-[90vh] bg-white dark:bg-boogle-dark rounded-2xl shadow-2xl overflow-hidden animate-scale-in"
        onClick={(e) => e.stopPropagation()}
      >
        {isLoading ? (
          <div className="h-[60vh] flex items-center justify-center">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-gray-900 dark:border-white"></div>
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
  );
}
