import { useState, useEffect, useRef } from "react";

interface CachedImageProps extends React.ImgHTMLAttributes<HTMLImageElement> {
    src: string;
}

export function CachedImage({ src, alt, className, ...props }: CachedImageProps) {
    const [imageSrc, setImageSrc] = useState<string | null>(null);
    const [isLoading, setIsLoading] = useState(true);
    const objectUrlRef = useRef<string | null>(null);

    useEffect(() => {
        let isMounted = true;
        setIsLoading(true);

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

                    // Try direct fetch only if not a known non-CORS domain
                    if (!isKnownNoCors) {
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
                        throw new Error("Network response was not ok");
                    }
                }
            } catch (error) {
                console.warn("Image caching failed, falling back to original src:", error);

                // Fallback to original src
                if (isMounted) {
                    setImageSrc(src);
                    setIsLoading(false);
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
    }, [src]);

    if (isLoading && !imageSrc) {
        return <div className={`bg-gray-200 dark:bg-white/10 animate-pulse rounded ${className}`} />;
    }

    return (
        <img
            src={imageSrc || src}
            alt={alt}
            className={className}
            {...props}
        />
    );
}
