import type { LoaderFunctionArgs } from "react-router";

// Security: Only allow proxying from trusted image sources
const ALLOWED_DOMAINS = [
  "gutenberg.org",
  "www.gutenberg.org",
  "openlibrary.org",
  "covers.openlibrary.org",
];

// Security: Block private/internal IP ranges to prevent SSRF
const BLOCKED_IP_PATTERNS = [
  /^127\./, // Loopback
  /^10\./, // Private Class A
  /^172\.(1[6-9]|2[0-9]|3[01])\./, // Private Class B
  /^192\.168\./, // Private Class C
  /^169\.254\./, // Link-local
  /^0\./, // Current network
  /^::1$/, // IPv6 loopback
  /^fc00:/i, // IPv6 unique local
  /^fe80:/i, // IPv6 link-local
];

const MAX_IMAGE_SIZE = 10 * 1024 * 1024; // 10MB
const FETCH_TIMEOUT = 30000; // 30 seconds
const ALLOWED_CONTENT_TYPES = [
  "image/jpeg",
  "image/jpg",
  "image/png",
  "image/gif",
  "image/webp",
  "image/svg+xml",
  "image/bmp",
  "image/tiff",
];

function isValidImageUrl(urlString: string): { valid: boolean; error?: string } {
  let parsed: URL;
  
  try {
    parsed = new URL(urlString);
  } catch {
    return { valid: false, error: "Invalid URL format" };
  }

  // Security: Only allow http/https protocols
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    return { valid: false, error: "Only HTTP/HTTPS protocols allowed" };
  }

  // Security: Validate hostname is in allowlist
  const hostname = parsed.hostname.toLowerCase();
  const isAllowed = ALLOWED_DOMAINS.some(domain => 
    hostname === domain || hostname.endsWith(`.${domain}`)
  );
  
  if (!isAllowed) {
    return { valid: false, error: "Domain not in allowlist" };
  }

  // Security: Check for IP-based URLs that might bypass DNS
  const isIpAddress = /^\d+\.\d+\.\d+\.\d+$/.test(hostname) || /^[0-9a-f:]+$/i.test(hostname);
  if (isIpAddress) {
    return { valid: false, error: "IP addresses not allowed" };
  }

  return { valid: true };
}

export async function loader({ request }: LoaderFunctionArgs) {
  const url = new URL(request.url);
  const imageUrl = url.searchParams.get("url");

  if (!imageUrl) {
    return new Response("Missing url param", { status: 400 });
  }

  // Security: Validate URL before fetching
  const validation = isValidImageUrl(imageUrl);
  if (!validation.valid) {
    return new Response(validation.error, { status: 403 });
  }

  try {
    // Security: Abort controller for timeout
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), FETCH_TIMEOUT);

    const response = await fetch(imageUrl, {
      headers: {
        "User-Agent": "BoogleBot/1.0",
      },
      signal: controller.signal,
    });

    clearTimeout(timeoutId);

    if (!response.ok) {
      return new Response("Failed to fetch image", { status: response.status });
    }

    // Security: Validate content-type is an image
    const contentType = response.headers.get("Content-Type") || "";
    const normalizedContentType = contentType.toLowerCase().split(";")[0].trim();
    
    if (!ALLOWED_CONTENT_TYPES.includes(normalizedContentType)) {
      return new Response("Invalid content type", { status: 403 });
    }

    // Security: Check content length if provided
    const contentLength = response.headers.get("Content-Length");
    if (contentLength) {
      const size = parseInt(contentLength, 10);
      if (isNaN(size) || size > MAX_IMAGE_SIZE) {
        return new Response("Image too large", { status: 413 });
      }
    }

    // Security: Stream with size limit check
    const clonedResponse = response.clone();
    const blob = await clonedResponse.blob();
    
    if (blob.size > MAX_IMAGE_SIZE) {
      return new Response("Image too large", { status: 413 });
    }

    // Success: Return with security headers
    return new Response(blob, {
      headers: {
        "Content-Type": normalizedContentType,
        "Cache-Control": "public, max-age=31536000, immutable",
        "Access-Control-Allow-Origin": "*",
        "X-Content-Type-Options": "nosniff",
      },
    });
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      return new Response("Request timeout", { status: 504 });
    }
    
    // Security: Don't leak internal error details
    console.error("Proxy error:", error);
    return new Response("Internal Server Error", { status: 500 });
  }
}
