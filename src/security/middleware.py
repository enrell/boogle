"""
Security middleware for FastAPI application.

Provides request-level security controls:
- Rate limiting
- Security headers
- Request size limits
- Suspicious pattern detection
"""

from fastapi import Request, Response, HTTPException
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
import time
import re
from typing import Optional


class SecurityMiddleware(BaseHTTPMiddleware):
    """Security middleware for API protection."""

    def __init__(
        self,
        app,
        max_request_size: int = 10 * 1024 * 1024,  # 10MB
        max_requests_per_minute: int = 100,
    ):
        super().__init__(app)
        self.max_request_size = max_request_size
        self.max_requests_per_minute = max_requests_per_minute
        self.request_log: dict[str, list[float]] = {}

    async def dispatch(self, request: Request, call_next) -> Response:
        # Check request size
        if "content-length" in request.headers:
            content_length = int(request.headers["content-length"])
            if content_length > self.max_request_size:
                return JSONResponse(
                    status_code=413, content={"detail": "Request too large"}
                )

        # Check for suspicious patterns
        if self._is_suspicious_request(request):
            return JSONResponse(
                status_code=403, content={"detail": "Suspicious request detected"}
            )

        # Rate limiting
        client_ip = request.client.host if request.client else "unknown"
        if not self._check_rate_limit(client_ip):
            return JSONResponse(
                status_code=429, content={"detail": "Rate limit exceeded"}
            )

        # Process the request
        response = await call_next(request)

        # Add security headers
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Strict-Transport-Security"] = (
            "max-age=31536000; includeSubDomains"
        )
        response.headers["Content-Security-Policy"] = "default-src 'self'"

        return response

    def _is_suspicious_request(self, request: Request) -> bool:
        """Check for suspicious patterns in request."""
        # Check URL for path traversal attempts
        url = str(request.url)
        suspicious_patterns = [
            r"\.\./",  # Path traversal
            r"\.\.\\",  # Windows path traversal
            r"%2e%2e",  # URL encoded ..
            r"<script",  # XSS attempt
            r"javascript:",  # XSS attempt
            r"on\w+=",  # Event handler injection
        ]

        for pattern in suspicious_patterns:
            if re.search(pattern, url, re.IGNORECASE):
                return True

        return False

    def _check_rate_limit(self, client_ip: str) -> bool:
        """Check if client is within rate limit."""
        now = time.time()
        minute_ago = now - 60

        if client_ip not in self.request_log:
            self.request_log[client_ip] = []

        # Clean old requests
        self.request_log[client_ip] = [
            t for t in self.request_log[client_ip] if t > minute_ago
        ]

        # Check limit
        if len(self.request_log[client_ip]) >= self.max_requests_per_minute:
            return False

        # Log this request
        self.request_log[client_ip].append(now)
        return True
