"""
Security validators and sanitizers.

Prevents common security vulnerabilities:
- SQL Injection
- XSS (Cross-Site Scripting)
- Path Traversal
- Command Injection
"""

import re
import html
from typing import Optional, List
from urllib.parse import urlparse


class SecurityError(ValueError):
    """Raised when security validation fails."""

    pass


class SecurityValidators:
    """Security validation and sanitization utilities."""

    # Maximum lengths for safety
    MAX_TITLE_LENGTH = 5000
    MAX_AUTHOR_LENGTH = 500
    MAX_QUERY_LENGTH = 1000
    MAX_DESCRIPTION_LENGTH = 50000
    MAX_BOOK_ID_LENGTH = 100

    # Dangerous patterns
    SQL_PATTERNS = [
        r"(\b(SELECT|INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|EXEC|UNION)\b)",
        r"(\b(OR|AND)\s+\d+=\d+)",
        r"(--|#|/\*|\*/)",
        r"(\b(SLEEP|BENCHMARK|WAITFOR|DELAY)\b)",
    ]

    XSS_PATTERNS = [
        r"<script[^>]*>[\s\S]*?</script>",
        r"<[^>]+on\w+\s*=\s*['\"]?[^>]*>",
        r"javascript:",
        r"vbscript:",
        r"data:text/html",
        r"<iframe[^>]*>",
        r"<object[^>]*>",
        r"<embed[^>]*>",
    ]

    PATH_TRAVERSAL_PATTERNS = [
        r"\.\./",
        r"\.\.\\",
        r"%2e%2e/",
        r"%252e%252e/",
    ]

    @classmethod
    def contains_sql_injection(cls, value: str) -> bool:
        """Check if value contains SQL injection patterns."""
        if not value:
            return False

        upper_value = value.upper()
        for pattern in cls.SQL_PATTERNS:
            if re.search(pattern, upper_value, re.IGNORECASE):
                return True
        return False

    @classmethod
    def contains_xss(cls, value: str) -> bool:
        """Check if value contains XSS patterns."""
        if not value:
            return False

        for pattern in cls.XSS_PATTERNS:
            if re.search(pattern, value, re.IGNORECASE):
                return True
        return False

    @classmethod
    def contains_path_traversal(cls, value: str) -> bool:
        """Check if value contains path traversal patterns."""
        if not value:
            return False

        for pattern in cls.PATH_TRAVERSAL_PATTERNS:
            if re.search(pattern, value):
                return True
        return False

    @classmethod
    def sanitize_string(
        cls, value: Optional[str], max_length: int = 1000
    ) -> Optional[str]:
        """
        Sanitize a string value.

        - HTML escapes dangerous characters
        - Removes null bytes
        - Truncates to max_length
        - Normalizes whitespace
        """
        if value is None:
            return None

        # Convert to string
        value = str(value)

        # Remove null bytes
        value = value.replace("\x00", "")

        # HTML escape
        value = html.escape(value)

        # Normalize whitespace (but preserve \n, \t)
        value = re.sub(r"[\r\f\v]", "", value)

        # Truncate if too long
        if len(value) > max_length:
            value = value[:max_length] + "..."

        return value

    @classmethod
    def sanitize_url(
        cls, url: Optional[str], allowed_schemes: List[str] = None
    ) -> Optional[str]:
        """
        Sanitize and validate a URL.

        Only allows http:// and https:// by default.
        Rejects javascript:, data:, file: etc.
        """
        if url is None:
            return None

        if allowed_schemes is None:
            allowed_schemes = ["http", "https"]

        try:
            parsed = urlparse(url)

            # Check scheme
            if parsed.scheme not in allowed_schemes:
                raise SecurityError(f"URL scheme '{parsed.scheme}' not allowed")

            # Check for path traversal
            if cls.contains_path_traversal(parsed.path):
                raise SecurityError("URL contains path traversal")

            return url
        except Exception as e:
            if isinstance(e, SecurityError):
                raise
            raise SecurityError(f"Invalid URL: {e}")

    @classmethod
    def validate_book_id(cls, book_id: str) -> str:
        """
        Validate book ID.

        Book IDs should be alphanumeric with limited special characters.
        """
        if not book_id:
            raise SecurityError("Book ID cannot be empty")

        if len(book_id) > cls.MAX_BOOK_ID_LENGTH:
            raise SecurityError(f"Book ID too long (max {cls.MAX_BOOK_ID_LENGTH})")

        # Allow alphanumeric, hyphens, underscores, colons
        if not re.match(r"^[\w\-:]+$", book_id):
            raise SecurityError("Book ID contains invalid characters")

        if cls.contains_sql_injection(book_id):
            raise SecurityError("Book ID contains SQL injection")

        return book_id

    @classmethod
    def validate_provider(cls, provider: str) -> str:
        """
        Validate provider name.

        Provider names should be lowercase alphanumeric.
        """
        if not provider:
            raise SecurityError("Provider cannot be empty")

        # Should be lowercase, alphanumeric, underscores
        if not re.match(r"^[a-z][a-z0-9_]*$", provider):
            raise SecurityError("Provider name must be lowercase alphanumeric")

        return provider

    @classmethod
    def validate_query(cls, query: str) -> str:
        """
        Validate search query.

        - Checks for SQL injection
        - Limits length
        - Removes dangerous characters
        """
        if not query:
            return ""

        # Convert to string
        query = str(query)

        if len(query) > cls.MAX_QUERY_LENGTH:
            raise SecurityError(f"Query too long (max {cls.MAX_QUERY_LENGTH})")

        # Check for SQL injection
        if cls.contains_sql_injection(query):
            raise SecurityError("Query contains SQL injection patterns")

        # Check for script tags
        if cls.contains_xss(query):
            # Remove script tags but keep the text
            query = re.sub(
                r"<script[^>]*>[\s\S]*?</script>", "", query, flags=re.IGNORECASE
            )

        return query.strip()

    @classmethod
    def validate_title(cls, title: str) -> str:
        """Validate and sanitize book title."""
        if not title:
            raise SecurityError("Title cannot be empty")

        if len(title) > cls.MAX_TITLE_LENGTH:
            raise SecurityError(f"Title too long (max {cls.MAX_TITLE_LENGTH})")

        # Sanitize
        title = cls.sanitize_string(title, cls.MAX_TITLE_LENGTH)

        return title

    @classmethod
    def validate_author(cls, author: Optional[str]) -> Optional[str]:
        """Validate and sanitize author name."""
        if not author:
            return None

        if len(author) > cls.MAX_AUTHOR_LENGTH:
            raise SecurityError(f"Author name too long (max {cls.MAX_AUTHOR_LENGTH})")

        return cls.sanitize_string(author, cls.MAX_AUTHOR_LENGTH)

    @classmethod
    def validate_description(cls, description: Optional[str]) -> Optional[str]:
        """Validate and sanitize description."""
        if not description:
            return None

        if len(description) > cls.MAX_DESCRIPTION_LENGTH:
            raise SecurityError(
                f"Description too long (max {cls.MAX_DESCRIPTION_LENGTH})"
            )

        # Sanitize (allows some HTML like <p>, <br> but escapes scripts)
        description = cls.sanitize_string(description, cls.MAX_DESCRIPTION_LENGTH)

        return description

    @classmethod
    def validate_integer(
        cls, value: str, min_val: Optional[int] = None, max_val: Optional[int] = None
    ) -> int:
        """Validate and parse integer."""
        try:
            num = int(value)
        except (ValueError, TypeError):
            raise SecurityError(f"Invalid integer: {value}")

        if min_val is not None and num < min_val:
            raise SecurityError(f"Value too small (min {min_val})")

        if max_val is not None and num > max_val:
            raise SecurityError(f"Value too large (max {max_val})")

        return num


# Convenience functions
def sanitize_string(value: Optional[str], max_length: int = 1000) -> Optional[str]:
    """Sanitize a string."""
    return SecurityValidators.sanitize_string(value, max_length)


def sanitize_url(
    url: Optional[str], allowed_schemes: List[str] = None
) -> Optional[str]:
    """Sanitize a URL."""
    return SecurityValidators.sanitize_url(url, allowed_schemes)


def validate_book_id(book_id: str) -> str:
    """Validate book ID."""
    return SecurityValidators.validate_book_id(book_id)


def validate_provider(provider: str) -> str:
    """Validate provider name."""
    return SecurityValidators.validate_provider(provider)


def validate_query(query: str) -> str:
    """Validate search query."""
    return SecurityValidators.validate_query(query)
