// Client-side API - re-export types only
// Actual API calls happen server-side via loaders

export type { 
  BookMetadata, 
  SearchResponse, 
  BookSearchResult,
  SearchResult,
  SearchMeta,
  SearchEnhancements,
  Author,
  SourceInfo,
  FileInfo,
} from "./api.server";
