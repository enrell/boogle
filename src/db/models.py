"""
SQLAlchemy Models for Boogle
"""

from datetime import datetime
from typing import Optional
from sqlalchemy import String, Integer, Float, BigInteger, Text, JSON, Index
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    pass


class Book(Base):
    __tablename__ = "books"
    
    # Primary key
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    # Core metadata
    source: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    book_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[Optional[str]] = mapped_column(Text)
    author: Mapped[Optional[str]] = mapped_column(Text)
    illustrator: Mapped[Optional[str]] = mapped_column(Text)
    release_date: Mapped[Optional[str]] = mapped_column(String(100))
    language: Mapped[Optional[str]] = mapped_column(String(50))
    category: Mapped[Optional[str]] = mapped_column(Text)
    original_publication: Mapped[Optional[str]] = mapped_column(Text)
    credits: Mapped[Optional[str]] = mapped_column(Text)
    copyright_status: Mapped[Optional[str]] = mapped_column(String(100))
    downloads: Mapped[Optional[str]] = mapped_column(String(50))
    cover_url: Mapped[Optional[str]] = mapped_column(Text)
    files: Mapped[list] = mapped_column(JSON, nullable=False, default=list, server_default='[]')
    
    # Enrichment metadata from Open Library
    ratings_average: Mapped[Optional[float]] = mapped_column(Float)
    ratings_count: Mapped[Optional[int]] = mapped_column(BigInteger)
    want_to_read_count: Mapped[Optional[int]] = mapped_column(BigInteger)
    edition_count: Mapped[Optional[int]] = mapped_column(Integer)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())
    
    __table_args__ = (
        Index('idx_books_source_book_id', 'source', 'book_id', unique=True),
        Index('idx_books_author_lower', func.lower(author)),
        Index('idx_books_title_lower', func.lower(title)),
    )
    
    def __repr__(self):
        return f"<Book(id={self.id}, title='{self.title}', author='{self.author}')>"
    
    def to_dict(self) -> dict:
        """Convert to dictionary for API responses"""
        return {
            'source': self.source,
            'book_id': self.book_id,
            'url': self.url,
            'title': self.title,
            'author': self.author,
            'illustrator': self.illustrator,
            'release_date': self.release_date,
            'language': self.language,
            'category': self.category,
            'original_publication': self.original_publication,
            'credits': self.credits,
            'copyright_status': self.copyright_status,
            'downloads': self.downloads,
            'cover_url': self.cover_url,
            'files': self.files or [],
            'ratings_average': self.ratings_average,
            'ratings_count': self.ratings_count,
            'want_to_read_count': self.want_to_read_count,
            'edition_count': self.edition_count,
        }


class SeedOffset(Base):
    __tablename__ = "seed_offsets"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(50), nullable=False, unique=True)
    position: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    last_book_id: Mapped[Optional[str]] = mapped_column(String(100))
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())
    
    def __repr__(self):
        return f"<SeedOffset(source='{self.source}', offset={self.last_offset})>"
