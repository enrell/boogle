"""
Evaluation Dataset Management

Create and manage evaluation datasets with queries and ground truth.
"""

import json
import random
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Optional, Any


@dataclass
class QuerySample:
    """A single query sample for evaluation."""

    query_id: str
    query_text: str
    category: str  # e.g., "title", "author", "subject", "phrase"
    expected_results: Optional[List[str]] = None  # Expected doc_ids
    notes: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "QuerySample":
        return cls(**data)


class EvaluationDataset:
    """
    Manages evaluation datasets with queries and ground truth.

    Example:
        dataset = EvaluationDataset()

        # Add queries
        dataset.add_query(QuerySample(
            query_id="q1",
            query_text="Hamlet",
            category="title",
            expected_results=["gutenberg:1524"]
        ))

        # Save
        dataset.save("data/evaluation/test_queries.json")
    """

    def __init__(self, name: str = "default"):
        self.name = name
        self.queries: List[QuerySample] = []
        self.metadata: Dict[str, Any] = {
            "name": name,
            "description": "",
            "num_queries": 0,
        }

    def add_query(self, query: QuerySample):
        """Add a query to the dataset."""
        self.queries.append(query)
        self.metadata["num_queries"] = len(self.queries)

    def add_queries(self, queries: List[QuerySample]):
        """Add multiple queries."""
        self.queries.extend(queries)
        self.metadata["num_queries"] = len(self.queries)

    def get_queries_by_category(self, category: str) -> List[QuerySample]:
        """Get all queries in a category."""
        return [q for q in self.queries if q.category == category]

    def get_query_by_id(self, query_id: str) -> Optional[QuerySample]:
        """Get a query by ID."""
        for q in self.queries:
            if q.query_id == query_id:
                return q
        return None

    def save(self, filepath: str):
        """Save dataset to JSON file."""
        data = {
            "metadata": self.metadata,
            "queries": [q.to_dict() for q in self.queries],
        }

        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    @classmethod
    def load(cls, filepath: str) -> "EvaluationDataset":
        """Load dataset from JSON file."""
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        dataset = cls(data["metadata"].get("name", "default"))
        dataset.metadata = data.get("metadata", {})

        for q_data in data.get("queries", []):
            dataset.add_query(QuerySample.from_dict(q_data))

        return dataset

    def split(self, train_ratio: float = 0.8, seed: int = 42) -> tuple:
        """Split dataset into train and test sets."""
        random.seed(seed)
        queries = self.queries.copy()
        random.shuffle(queries)

        split_idx = int(len(queries) * train_ratio)

        train_dataset = EvaluationDataset(f"{self.name}_train")
        train_dataset.metadata = {**self.metadata, "split": "train"}
        train_dataset.add_queries(queries[:split_idx])

        test_dataset = EvaluationDataset(f"{self.name}_test")
        test_dataset.metadata = {**self.metadata, "split": "test"}
        test_dataset.add_queries(queries[split_idx:])

        return train_dataset, test_dataset

    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about the dataset."""
        categories = {}
        for q in self.queries:
            categories[q.category] = categories.get(q.category, 0) + 1

        return {
            "total_queries": len(self.queries),
            "categories": categories,
            "has_expected_results": sum(1 for q in self.queries if q.expected_results),
        }


def create_comprehensive_evaluation_dataset() -> EvaluationDataset:
    """
    Create a comprehensive evaluation dataset with diverse query types.

    Query categories:
    - title: Book titles
    - author: Author names
    - subject: Subject/genre queries
    - phrase: Phrase or quote searches
    - typo: Intentional typos
    - partial: Partial matches
    """
    dataset = EvaluationDataset("boogle_comprehensive_v1")
    dataset.metadata["description"] = (
        "Comprehensive evaluation dataset for Boogle search"
    )

    # Title queries
    title_queries = [
        QuerySample("t1", "Hamlet", "title", notes="Exact title match"),
        QuerySample("t2", "Pride and Prejudice", "title", notes="Classic novel"),
        QuerySample("t3", "The Great Gatsby", "title", notes="20th century American"),
        QuerySample("t4", "Moby Dick", "title", notes="Short title"),
        QuerySample("t5", "War and Peace", "title", notes="Multi-word title"),
        QuerySample("t6", "Adventures of Sherlock Holmes", "title", notes="Long title"),
        QuerySample("t7", "Dom Casmurro", "title", notes="Portuguese title"),
        QuerySample("t8", "Les Misérables", "title", notes="Foreign title with accent"),
    ]

    # Author queries
    author_queries = [
        QuerySample("a1", "William Shakespeare", "author", notes="Famous playwright"),
        QuerySample("a2", "Jane Austen", "author", notes="Romance author"),
        QuerySample("a3", "Charles Dickens", "author", notes="Victorian author"),
        QuerySample("a4", "Mark Twain", "author", notes="American humorist"),
        QuerySample("a5", "Machado de Assis", "author", notes="Brazilian author"),
        QuerySample("a6", "Victor Hugo", "author", notes="French author"),
    ]

    # Subject/genre queries
    subject_queries = [
        QuerySample("s1", "Romance novels", "subject", notes="Genre"),
        QuerySample("s2", "Science fiction", "subject", notes="Genre"),
        QuerySample("s3", "Mystery stories", "subject", notes="Genre"),
        QuerySample("s4", "Philosophy", "subject", notes="Academic subject"),
        QuerySample("s5", "Physics", "subject", notes="Science"),
        QuerySample("s6", "Poetry", "subject", notes="Literary form"),
        QuerySample("s7", "History", "subject", notes="Non-fiction"),
        QuerySample("s8", "Children's books", "subject", notes="Age category"),
    ]

    # Phrase/quote queries
    phrase_queries = [
        QuerySample("p1", "To be or not to be", "phrase", notes="Famous quote"),
        QuerySample(
            "p2", "It was the best of times", "phrase", notes="Dickens opening"
        ),
        QuerySample("p3", "Call me Ishmael", "phrase", notes="Melville opening"),
        QuerySample("p4", "All happy families", "phrase", notes="Tolstoy opening"),
    ]

    # Typo queries (for spell correction testing)
    typo_queries = [
        QuerySample("ty1", "Shakspeare", "typo", notes="Missing 'e'"),
        QuerySample("ty2", "Dickins", "typo", notes="Missing 'e'"),
        QuerySample("ty3", "Austen", "typo", notes="Correct spelling"),
        QuerySample("ty4", "Machado de Assiz", "typo", notes="Wrong ending"),
    ]

    # Partial match queries
    partial_queries = [
        QuerySample("pa1", "Pride", "partial", notes="Partial title"),
        QuerySample("pa2", "Gatsby", "partial", notes="Last name only"),
        QuerySample("pa3", "Holmes", "partial", notes="Character name"),
        QuerySample("pa4", "Sherlock", "partial", notes="First name"),
    ]

    # Add all queries
    dataset.add_queries(title_queries)
    dataset.add_queries(author_queries)
    dataset.add_queries(subject_queries)
    dataset.add_queries(phrase_queries)
    dataset.add_queries(typo_queries)
    dataset.add_queries(partial_queries)

    return dataset


def create_quick_test_dataset() -> EvaluationDataset:
    """Create a small dataset for quick testing (10 queries)."""
    dataset = EvaluationDataset("boogle_quick_test")

    queries = [
        QuerySample("q1", "Hamlet", "title"),
        QuerySample("q2", "Shakespeare", "author"),
        QuerySample("q3", "Romance", "subject"),
        QuerySample("q4", "Shakspeare", "typo"),
        QuerySample("q5", "Pride", "partial"),
    ]

    dataset.add_queries(queries)
    return dataset


if __name__ == "__main__":
    # Create and save comprehensive dataset
    print("Creating comprehensive evaluation dataset...")
    dataset = create_comprehensive_evaluation_dataset()

    print(f"Total queries: {len(dataset.queries)}")

    stats = dataset.get_statistics()
    print(f"\nStatistics:")
    for category, count in stats["categories"].items():
        print(f"  {category}: {count} queries")

    # Save
    output_file = "data/evaluation/test_queries.json"
    dataset.save(output_file)
    print(f"\nSaved to: {output_file}")

    # Create quick test dataset
    print("\nCreating quick test dataset...")
    quick = create_quick_test_dataset()
    quick.save("data/evaluation/quick_test_queries.json")
    print("Saved to: data/evaluation/quick_test_queries.json")
