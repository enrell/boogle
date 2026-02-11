"""
Browser-based downloader using Camoufox for sites requiring JavaScript/session handling.

Provides async concurrent downloads with retry logic and PDF-to-text extraction.
"""

import asyncio
import tempfile
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
import re
import time
from concurrent.futures import ThreadPoolExecutor

try:
    from camoufox import AsyncCamoufox

    CAMOUFOX_AVAILABLE = True
except ImportError:
    CAMOUFOX_AVAILABLE = False


class BrowserDownloader:
    """
    Browser-based downloader with concurrent downloads and retry logic.
    """

    def __init__(self, headless: bool = True, timeout: int = 30, max_workers: int = 4):
        self.headless = headless
        self.timeout = timeout
        self.max_workers = max_workers

    async def download_batch(
        self,
        downloads: List[Tuple[str, Path]],
        retry_attempts: int = 3,
        retry_delay: float = 2.0,
    ) -> List[Tuple[str, Path, Optional[Path]]]:
        """
        Download multiple files concurrently with retry logic.

        Args:
            downloads: List of (url, output_path) tuples
            retry_attempts: Number of retry attempts per download
            retry_delay: Delay between retries in seconds

        Returns:
            List of (url, output_path, result_path) tuples
        """
        if not CAMOUFOX_AVAILABLE:
            return [(url, out, None) for url, out in downloads]

        semaphore = asyncio.Semaphore(self.max_workers)

        async def download_with_retry(
            url: str, output_path: Path
        ) -> Tuple[str, Path, Optional[Path]]:
            async with semaphore:
                for attempt in range(retry_attempts):
                    try:
                        result = await self._download_single(url, output_path)
                        if result:
                            return url, output_path, result
                    except Exception as e:
                        if attempt < retry_attempts - 1:
                            await asyncio.sleep(retry_delay * (attempt + 1))
                        continue
                return url, output_path, None

        tasks = [download_with_retry(url, out) for url, out in downloads]
        return await asyncio.gather(*tasks)

    async def _download_single(self, url: str, output_path: Path) -> Optional[Path]:
        """Download a single file."""
        if not CAMOUFOX_AVAILABLE:
            return None

        try:
            async with AsyncCamoufox() as fox:
                page = await fox.new_page()

                # Navigate to the URL
                await page.goto(url, timeout=self.timeout * 1000)
                await page.wait_for_load_state("networkidle")

                # Check if this is a direct PDF link
                content_type = await page.evaluate(
                    '() => document.contentType || "text/html"'
                )

                if "pdf" in content_type.lower():
                    # Direct PDF - download it
                    pdf_data = await page.pdf()
                    output_path.write_bytes(pdf_data)
                    return output_path
                else:
                    # HTML page - try to find download link
                    download_link = await page.evaluate("""() => {
                        const links = document.querySelectorAll('a[href*=".pdf"], a[download]');
                        for (const link of links) {
                            if (link.href && link.href.includes('.pdf')) {
                                return link.href;
                            }
                        }
                        const iframes = document.querySelectorAll('iframe[src*=".pdf"]');
                        if (iframes.length > 0) {
                            return iframes[0].src;
                        }
                        return null;
                    }""")

                    if download_link:
                        await page.goto(download_link, timeout=self.timeout * 1000)
                        await asyncio.sleep(1)
                        pdf_data = await page.pdf()
                        output_path.write_bytes(pdf_data)
                        return output_path

        except Exception:
            pass

        return None


def extract_text_from_pdf(pdf_path: Path, txt_path: Path) -> bool:
    """
    Extract text from PDF and save as .txt file.

    Args:
        pdf_path: Path to PDF file
        txt_path: Path to save text file

    Returns:
        True if extraction successful
    """
    try:
        # Try PyPDF2 first
        try:
            import PyPDF2

            with open(pdf_path, "rb") as pdf_file:
                reader = PyPDF2.PdfReader(pdf_file)
                text = []
                for page in reader.pages:
                    text.append(page.extract_text())
                full_text = "\n".join(filter(None, text))
                if full_text.strip():
                    txt_path.write_text(full_text, encoding="utf-8")
                    return True
        except ImportError:
            pass

        # Try pdfplumber
        try:
            import pdfplumber

            with pdfplumber.open(pdf_path) as pdf:
                text = []
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text.append(page_text)
                full_text = "\n".join(text)
                if full_text.strip():
                    txt_path.write_text(full_text, encoding="utf-8")
                    return True
        except ImportError:
            pass

        # Try pdfminer.six
        try:
            from pdfminer.high_level import extract_text

            text = extract_text(str(pdf_path))
            if text.strip():
                txt_path.write_text(text, encoding="utf-8")
                return True
        except ImportError:
            pass

    except Exception:
        pass

    return False


def sanitize_filename(name: str) -> str:
    """Sanitize filename by removing invalid characters."""
    sanitized = re.sub(r'[<>:"/\\|?*]', "_", name)
    sanitized = re.sub(r"[\x00-\x1f\x7f-\x9f]", "", sanitized)
    if len(sanitized) > 200:
        sanitized = sanitized[:200]
    return sanitized.strip()


def download_with_browser(
    url: str, output_path: Path, headless: bool = True, timeout: int = 30
) -> Optional[Path]:
    """Synchronous wrapper for single download."""
    if not CAMOUFOX_AVAILABLE:
        return None

    async def _download():
        downloader = BrowserDownloader(headless=headless, timeout=timeout)
        return await downloader._download_single(url, output_path)

    try:
        return asyncio.get_event_loop().run_until_complete(_download())
    except Exception:
        return None
