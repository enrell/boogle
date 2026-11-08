import { useState, useEffect, useRef } from "react";

interface CachedImageProps extends React.ImgHTMLAttributes<HTMLImageElement> {
  src: string;
  fallbackSrc?: string;
  onError?: () => void;
}

export function CachedImage({ 
  src, 
  alt, 
  className, 
  fallbackSrc,
  onError,
  ...props 
}: CachedImageProps) {
  const [imageSrc, setImageSrc] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [hasError, setHasError] = useState(false);
  const objectUrlRef = useRef<string | null>(null);

  useEffect(() => {
    let isMounted = true;
    setIsLoading(true);
    setHasError(false);

    const fetchAndCacheImage = async () => {
      // If we have a previous object URL, revoke it
      if (objectUrlRef.current) {
        URL.revokeObjectURL(objectUrlRef.current);
        objectUrlRef.current = null;
      }

      try {
        // Check if Cache API is supported
        if (!('caches' in window)) {
          throw new Error("Cache API not supported");
        }

        const cache = await caches.open("boogle-image-cache");
        const cachedResponse = await cache.match(src);

        if (cachedResponse) {
          const blob = await cachedResponse.blob();
          const url = URL.createObjectURL(blob);
          objectUrlRef.current = url;
          if (isMounted) {
            setImageSrc(url);
            setIsLoading(false);
          }
        } else {
          let response;
          const isKnownNoCors = src.includes("gutenberg.org");
          const isOpenLibrary = src.includes("openlibrary.org");

          // Try direct fetch only if not a known non-CORS domain
          if (!isKnownNoCors && !isOpenLibrary) {
            try {
              response = await fetch(src, { mode: "cors" });
            } catch (e) {
              // Verify later if proxy is needed
            }
          }

          // If direct fetch didn't happen or failed, use proxy
          if (!response || !response.ok) {
            const proxyUrl = `/api/image-proxy?url=${encodeURIComponent(src)}`;
            response = await fetch(proxyUrl);
          }

          if (response.ok) {
            // Check if the response is actually an image (not a placeholder/error)
            const contentType = response.headers.get('content-type');
            if (contentType && contentType.startsWith('image/')) {
              // For OpenLibrary, check if it's a valid image (not the default "not found" image)
              if (isOpenLibrary) {
                // Clone response for checking
                const clonedResponse = response.clone();
                const blob = await clonedResponse.blob();
                
                // OpenLibrary returns a 1x1 GIF for missing covers, check the size
                if (blob.size < 100) {
                  throw new Error("Cover not available");
                }
              }
              
              // Cache the response
              cache.put(src, response.clone());
              const blob = await response.blob();
              const url = URL.createObjectURL(blob);
              objectUrlRef.current = url;
              if (isMounted) {
                setImageSrc(url);
                setIsLoading(false);
              }
            } else {
              throw new Error("Invalid content type");
            }
          } else {
            throw new Error("Network response was not ok");
          }
        }
      } catch (error) {
        console.warn("Image caching failed:", error);
        
        if (isMounted) {
          setHasError(true);
          setIsLoading(false);
          // Use fallback or original src
          setImageSrc(fallbackSrc || src);
          if (onError) onError();
        }
      }
    };

    if (src) {
      fetchAndCacheImage();
    }

    return () => {
      isMounted = false;
      if (objectUrlRef.current) {
        URL.revokeObjectURL(objectUrlRef.current);
      }
    };
  }, [src, fallbackSrc, onError]);

  const handleImageError = () => {
    if (!hasError) {
      setHasError(true);
      if (onError) onError();
    }
  };

  if (isLoading) {
    return (
      <div className={`bg-gray-200 dark:bg-white/10 animate-pulse ${className}`}>
        <div className="w-full h-full bg-gray-300 dark:bg-white/20 animate-pulse" />
      </div>
    );
  }

  if (hasError) {
    return (
      <div className={`bg-gray-100 dark:bg-white/5 flex flex-col items-center justify-center text-gray-400 dark:text-gray-600 ${className}`}>
        <svg className="w-8 h-8 mb-1 opacity-50" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M12 6.253v13m0-13C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253m0-13C13.168 5.477 14.754 5 16.5 5c1.747 0 3.332.477 4.5 1.253v13C19.832 18.477 18.247 18 16.5 18c-1.746 0-3.332.477-4.5 1.253" />
        </svg>
      </div>
    );
  }

  return (
    <img
      src={imageSrc || src}
      alt={alt}
      className={className}
      onError={handleImageError}
      {...props}
    />
  );
}
