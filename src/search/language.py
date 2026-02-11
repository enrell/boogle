"""
Language detection for multilingual book support.

Uses Lingua for high-accuracy detection with support for 75+ languages.
"""

from typing import Optional, List
from lingua import Language, LanguageDetectorBuilder


class LanguageDetector:
    """
    Detect language of text with confidence scores.
    Optimized for book content and queries.
    """

    # Languages commonly found in book collections
    COMMON_LANGUAGES = [
        Language.ENGLISH,
        Language.PORTUGUESE,
        Language.SPANISH,
        Language.FRENCH,
        Language.GERMAN,
        Language.ITALIAN,
        Language.DUTCH,
        Language.RUSSIAN,
        Language.CHINESE,
        Language.JAPANESE,
        Language.ARABIC,
        Language.HINDI,
    ]

    def __init__(self, languages: Optional[List[Language]] = None):
        """
        Initialize language detector.

        Args:
            languages: List of languages to detect (default: COMMON_LANGUAGES)
        """
        langs = languages or self.COMMON_LANGUAGES
        self.detector = LanguageDetectorBuilder.from_languages(*langs).build()

    def detect(self, text: str) -> Optional[str]:
        """
        Detect language of text.

        Args:
            text: Text to analyze

        Returns:
            ISO 639-1 language code (e.g., 'en', 'pt') or None
        """
        if not text or len(text.strip()) < 3:
            return None

        confidence_values = self.detector.compute_language_confidence_values(text)
        if not confidence_values:
            return None

        # Get most confident language - lingua returns ConfidenceValue objects
        most_confident = max(confidence_values, key=lambda x: x.value)

        # Only return if confidence is reasonable
        if most_confident.value > 0.1:
            return most_confident.language.iso_code_639_1.name.lower()

        return None

    def detect_with_confidence(self, text: str) -> Optional[tuple]:
        """
        Detect language with confidence score.

        Returns:
            Tuple of (language_code, confidence) or None
        """
        if not text or len(text.strip()) < 3:
            return None

        confidence_values = self.detector.compute_language_confidence_values(text)
        if not confidence_values:
            return None

        most_confident = max(confidence_values, key=lambda x: x.value)
        return (
            most_confident.language.iso_code_639_1.name.lower(),
            most_confident.value,
        )

    def detect_top_k(self, text: str, k: int = 3) -> List[tuple]:
        """
        Return top-k language predictions with scores.

        Returns:
            List of (language_code, confidence) tuples
        """
        if not text or len(text.strip()) < 3:
            return []

        confidence_values = self.detector.compute_language_confidence_values(text)
        sorted_values = sorted(confidence_values, key=lambda x: x.value, reverse=True)

        return [
            (cv.language.iso_code_639_1.name.lower(), cv.value)
            for cv in sorted_values[:k]
        ]


# ISO code to full name mapping for display
LANGUAGE_NAMES = {
    "en": "English",
    "pt": "Portuguese",
    "es": "Spanish",
    "fr": "French",
    "de": "German",
    "it": "Italian",
    "nl": "Dutch",
    "ru": "Russian",
    "zh": "Chinese",
    "ja": "Japanese",
    "ar": "Arabic",
    "hi": "Hindi",
}
