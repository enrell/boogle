"""
Relevance Annotator for creating ground truth data.

Interactive tool for labeling search results with relevance grades.
"""

import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict


@dataclass
class Annotation:
    """Single relevance annotation."""

    query_id: str
    doc_id: str
    relevance: int  # 0, 1, or 2
    notes: Optional[str] = None

    def to_dict(self):
        return asdict(self)


class RelevanceAnnotator:
    """
    Interactive tool for annotating search results.

    Creates ground truth data (qrels) for evaluation.

    Usage:
        annotator = RelevanceAnnotator()

        # Add annotations
        annotator.annotate("q1", "doc1", 2, "Perfect match")
        annotator.annotate("q1", "doc2", 1, "Related but not exact")
        annotator.annotate("q1", "doc3", 0, "Irrelevant")

        # Save
        annotator.save("data/evaluation/qrels.json")
    """

    def __init__(self):
        self.annotations: Dict[str, Dict[str, Annotation]] = {}

    def annotate(
        self, query_id: str, doc_id: str, relevance: int, notes: Optional[str] = None
    ):
        """
        Add a relevance annotation.

        Args:
            query_id: Query identifier
            doc_id: Document identifier
            relevance: 0=irrelevant, 1=somewhat relevant, 2=highly relevant
            notes: Optional notes
        """
        if query_id not in self.annotations:
            self.annotations[query_id] = {}

        self.annotations[query_id][doc_id] = Annotation(
            query_id=query_id, doc_id=doc_id, relevance=relevance, notes=notes
        )

    def get_annotation(self, query_id: str, doc_id: str) -> Optional[Annotation]:
        """Get annotation for a specific query-doc pair."""
        return self.annotations.get(query_id, {}).get(doc_id)

    def get_qrels(self) -> Dict[str, Dict[str, int]]:
        """
        Convert annotations to qrels format.

        Returns:
            {query_id: {doc_id: relevance}}
        """
        qrels = {}
        for query_id, docs in self.annotations.items():
            qrels[query_id] = {doc_id: ann.relevance for doc_id, ann in docs.items()}
        return qrels

    def save(self, filepath: str):
        """Save annotations to JSON file."""
        qrels = self.get_qrels()

        # Also save detailed annotations
        detailed = {
            query_id: {doc_id: ann.to_dict() for doc_id, ann in docs.items()}
            for query_id, docs in self.annotations.items()
        }

        output = {
            "qrels": qrels,
            "detailed_annotations": detailed,
        }

        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

    @classmethod
    def load(cls, filepath: str) -> "RelevanceAnnotator":
        """Load annotations from JSON file."""
        annotator = cls()

        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Load detailed annotations
        for query_id, docs in data.get("detailed_annotations", {}).items():
            for doc_id, ann_data in docs.items():
                annotator.annotate(
                    query_id=query_id,
                    doc_id=doc_id,
                    relevance=ann_data["relevance"],
                    notes=ann_data.get("notes"),
                )

        return annotator

    def get_statistics(self) -> Dict:
        """Get annotation statistics."""
        total = 0
        by_relevance = {0: 0, 1: 0, 2: 0}

        for query_annotations in self.annotations.values():
            for ann in query_annotations.values():
                total += 1
                by_relevance[ann.relevance] += 1

        return {
            "total_annotations": total,
            "num_queries": len(self.annotations),
            "by_relevance": {
                "irrelevant": by_relevance[0],
                "somewhat_relevant": by_relevance[1],
                "highly_relevant": by_relevance[2],
            },
            "relevance_distribution": {
                "irrelevant_pct": by_relevance[0] / total * 100 if total else 0,
                "somewhat_pct": by_relevance[1] / total * 100 if total else 0,
                "highly_pct": by_relevance[2] / total * 100 if total else 0,
            },
        }


def interactive_annotate(
    query: str, results: List[Dict], query_id: str = None
) -> Dict[str, int]:
    """
    Interactive annotation of search results.

    Args:
        query: Original query text
        results: List of search results
        query_id: Optional query identifier

    Returns:
        Dictionary mapping doc_id to relevance grade
    """
    print(f"\n{'=' * 70}")
    print(f"Annotating: '{query}'")
    print(f"{'=' * 70}\n")
    print("Relevance grades: 0=irrelevant, 1=somewhat, 2=highly relevant")
    print("Press Enter to skip, 'q' to quit\n")

    qrels = {}

    for i, result in enumerate(results[:20], 1):  # Annotate top 20
        doc_id = result.get("canonical_id", f"doc_{i}")
        title = result.get("title", "Unknown")
        author = result.get("authors", [{}])[0].get("name", "Unknown")

        print(f"\n{i}. {title}")
        print(f"   by {author}")

        while True:
            grade = input("   Relevance (0/1/2/s/q): ").strip().lower()

            if grade == "q":
                return qrels
            elif grade == "s":
                break  # Skip
            elif grade == "":
                break  # Skip
            elif grade in ["0", "1", "2"]:
                qrels[doc_id] = int(grade)
                break
            else:
                print("   Invalid input. Use 0, 1, 2, s (skip), or q (quit)")

    return qrels


if __name__ == "__main__":
    print("Relevance Annotator - use 'uv run boogle eval-annotate' to annotate")
