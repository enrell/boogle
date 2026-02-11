"""
Browser-based downloader using Camoufox for sites requiring JavaScript/session handling.

Provides a wrapper around Camoufox for downloading files from sites that
block direct HTTP requests.
"""

import asyncio
import tempfile
from pathlib import Path
from typing import Optional, Dict, Any
import re

try:
    from camoufox import AsyncCamoufox

    CAMOUFOX_AVAILABLE = True
except ImportError:
    CAMOUFOX_AVAILABLE = False


class BrowserDownloader:
    """
    Browser-based downloader for sites requiring session handling.

    Uses Camoufox for headless browser automation to download files
    from sites that require JavaScript, cookies, or session authentication.
    """

    def __init__(self, headless: bool = True, timeout: int = 30):
        """
        Initialize browser downloader.

        Args:
            headless: Run browser in headless mode (default: True)
            timeout: Timeout in seconds for page load (default: 30)
        """
        self.headless = headless
        self.timeout = timeout
        self._fox = None

    async def __aenter__(self):
        """Async context manager entry."""
        if CAMOUFOX_AVAILABLE:
            self._fox = await AsyncCamoufox().__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self._fox:
            await self._fox.__aexit__(exc_type, exc_val, exc_tb)

    async def download_file(
        self, url: str, output_path: Path, wait_for_download: bool = True
    ) -> Optional[Path]:
        """
        Download file using browser automation.

        Args:
            url: URL to download from
            output_path: Path to save the file
            wait_for_download: Wait for download to complete

        Returns:
            Path to downloaded file if successful, None otherwise
        """
        if not CAMOUFOX_AVAILABLE:
            return None

        try:
            async with AsyncCamoufox() as fox:
                page = await fox.new_page()

                # Navigate to the URL
                await page.goto(url, timeout=self.timeout * 1000)

                # Wait for page to load
                await page.wait_for_load_state("networkidle")

                # Check if this is a direct PDF link
                content_type = await page.evaluate("""() => {
                    return document.contentType || 'text/html';
                }""")

                if "pdf" in content_type.lower():
                    # Direct PDF - download it
                    pdf_data = await page.pdf()
                    output_path.write_bytes(pdf_data)
                    return output_path
                else:
                    # HTML page - try to find download link
                    download_link = await page.evaluate("""() => {
                        // Look for common download links
                        const links = document.querySelectorAll('a[href*=".pdf"], a[download], button[onclick*="download"]');
                        for (const link of links) {
                            if (link.href && link.href.includes('.pdf')) {
                                return link.href;
                            }
                        }
                        // Look for iframe with PDF
                        const iframes = document.querySelectorAll('iframe[src*=".pdf"]');
                        if (iframes.length > 0) {
                            return iframes[0].src;
                        }
                        return null;
                    }""")

                    if download_link:
                        # Navigate to the PDF
                        await page.goto(download_link, timeout=self.timeout * 1000)
                        await asyncio.sleep(1)  # Wait for PDF to load

                        # Try to download
                        pdf_data = await page.pdf()
                        output_path.write_bytes(pdf_data)
                        return output_path

                return None

        except Exception:
            return None

    async def download_with_session(
        self, url: str, output_path: Path, pre_actions: Optional[list] = None
    ) -> Optional[Path]:
        """
        Download with optional pre-actions (click, form submit, etc).

        Args:
            url: Initial URL
            output_path: Path to save file
            pre_actions: List of actions to perform before download
                        e.g., [{'action': 'click', 'selector': '#download-btn'}]

        Returns:
            Path to downloaded file if successful
        """
        if not CAMOUFOX_AVAILABLE:
            return None

        try:
            async with AsyncCamoufox() as fox:
                page = await fox.new_page()

                # Navigate to initial page
                await page.goto(url, timeout=self.timeout * 1000)
                await page.wait_for_load_state("networkidle")

                # Execute pre-actions
                if pre_actions:
                    for action in pre_actions:
                        if action.get("action") == "click":
                            selector = action.get("selector")
                            if selector:
                                await page.click(selector)
                                await asyncio.sleep(1)
                        elif action.get("action") == "wait":
                            await asyncio.sleep(action.get("seconds", 1))
                        elif action.get("action") == "goto":
                            await page.goto(
                                action.get("url"), timeout=self.timeout * 1000
                            )

                # Try to download current page as PDF
                pdf_data = await page.pdf()
                output_path.write_bytes(pdf_data)
                return output_path

        except Exception:
            return None


def sanitize_filename(name: str) -> str:
    """
    Sanitize filename by removing invalid characters.

    Args:
        name: Original filename

    Returns:
        Sanitized filename safe for filesystem
    """
    # Remove or replace invalid characters
    sanitized = re.sub(r'[<>:"/\\|?*]', "_", name)
    # Remove control characters
    sanitized = re.sub(r"[\x00-\x1f\x7f-\x9f]", "", sanitized)
    # Limit length
    if len(sanitized) > 200:
        sanitized = sanitized[:200]
    return sanitized.strip()


# Synchronous wrapper for easier usage
def download_with_browser(
    url: str, output_path: Path, headless: bool = True, timeout: int = 30
) -> Optional[Path]:
    """
    Synchronous wrapper for browser-based download.

    Args:
        url: URL to download from
        output_path: Path to save the file
        headless: Run browser in headless mode
        timeout: Timeout in seconds

    Returns:
        Path to downloaded file if successful, None otherwise
    """
    if not CAMOUFOX_AVAILABLE:
        return None

    async def _download():
        downloader = BrowserDownloader(headless=headless, timeout=timeout)
        async with downloader:
            return await downloader.download_file(url, output_path)

    try:
        return asyncio.get_event_loop().run_until_complete(_download())
    except Exception:
        return None
