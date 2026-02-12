"""
Boogle Search Evaluator using ir_measures.

Provides comprehensive evaluation of search ranking quality.
"""

import json
import statistics
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from enum import IntEnum

import ir_measures
from ir_measures import nDCG, MAP, RR


class RelevanceGrade(IntEnum):
    """Relevance grades for evaluation."""

    IRRELEVANT = 0
    SOMEWHAT_RELEVANT = 1
    HIGHLY_RELEVANT = 2


@dataclass
class EvaluationResult:
    """Result of a single query evaluation."""

    query_id: str
    query_text: str
    metrics: Dict[str, float]
    num_relevant: int
    num_retrieved: int

    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class AggregateMetrics:
    """Aggregate metrics across all queries."""

    ndcg_10: float
    ndcg_20: float
    map: float
    p_5: float
    p_10: float
    p_20: float
    r_10: float
    r_20: float
    r_100: float
    mrr: float
    num_queries: int

    def to_dict(self) -> Dict:
        return {
            "nDCG@10": round(self.ndcg_10, 4),
            "nDCG@20": round(self.ndcg_20, 4),
            "MAP": round(self.map, 4),
            "P@5": round(self.p_5, 4),
            "P@10": round(self.p_10, 4),
            "P@20": round(self.p_20, 4),
            "R@10": round(self.r_10, 4),
            "R@20": round(self.r_20, 4),
            "R@100": round(self.r_100, 4),
            "MRR": round(self.mrr, 4),
            "num_queries": self.num_queries,
        }


class BoogleEvaluator:
    """
    Comprehensive search evaluation system for Boogle.

    Uses ir_measures library for industry-standard IR metrics.
    """

    def __init__(self):
        """Initialize evaluator with standard metrics."""
        self.cutoffs = [5, 10, 20, 100]

    def evaluate(
        self,
        qrels: Dict[str, Dict[str, int]],
        runs: Dict[str, Dict[str, float]],
        query_texts: Optional[Dict[str, str]] = None,
    ) -> Tuple[AggregateMetrics, List[EvaluationResult]]:
        """
        Evaluate search results against relevance judgments.

        Args:
            qrels: {query_id: {doc_id: relevance_grade}}
            runs: {query_id: {doc_id: score}}
            query_texts: Optional mapping of query_id to original query text
        """
        # Convert to ir_measures format
        qrels_list = self._dict_to_measures_format(qrels)
        runs_list = self._dict_to_measures_format(runs)

        # Calculate metrics
        metrics_to_calc = [
            nDCG(cutoff=10),
            nDCG(cutoff=20),
            MAP,
            RR,
        ]

        # Add P and R at different cutoffs
        for cutoff in self.cutoffs:
            metrics_to_calc.extend(
                [
                    ir_measures.P(cutoff=cutoff),
                    ir_measures.R(cutoff=cutoff),
                ]
            )

        # Calculate aggregate metrics
        agg_results = ir_measures.calc_aggregate(metrics_to_calc, qrels_list, runs_list)

        # Build aggregate metrics object
        aggregate = AggregateMetrics(
            ndcg_10=agg_results.get(nDCG(cutoff=10), 0.0),
            ndcg_20=agg_results.get(nDCG(cutoff=20), 0.0),
            map=agg_results.get(MAP, 0.0),
            p_5=agg_results.get(ir_measures.P(cutoff=5), 0.0),
            p_10=agg_results.get(ir_measures.P(cutoff=10), 0.0),
            p_20=agg_results.get(ir_measures.P(cutoff=20), 0.0),
            r_10=agg_results.get(ir_measures.R(cutoff=10), 0.0),
            r_20=agg_results.get(ir_measures.R(cutoff=20), 0.0),
            r_100=agg_results.get(ir_measures.R(cutoff=100), 0.0),
            mrr=agg_results.get(RR, 0.0),
            num_queries=len(qrels),
        )

        # Calculate per-query metrics
        per_query_results = []
        per_query_metrics = ir_measures.iter_calc(
            metrics_to_calc, qrels_list, runs_list
        )

        # Group by query
        query_metrics = {}
        for metric, query_id, value in per_query_metrics:
            if query_id not in query_metrics:
                query_metrics[query_id] = {}
            query_metrics[query_id][str(metric)] = value

        for query_id, metrics in query_metrics.items():
            # Count relevant documents for this query
            num_relevant = sum(1 for r in qrels.get(query_id, {}).values() if r > 0)
            num_retrieved = len(runs.get(query_id, {}))

            result = EvaluationResult(
                query_id=query_id,
                query_text=query_texts.get(query_id, query_id)
                if query_texts
                else query_id,
                metrics=metrics,
                num_relevant=num_relevant,
                num_retrieved=num_retrieved,
            )
            per_query_results.append(result)

        return aggregate, per_query_results

    def evaluate_from_files(
        self, qrels_file: str, runs_file: str, query_texts_file: Optional[str] = None
    ) -> Tuple[AggregateMetrics, List[EvaluationResult]]:
        """Evaluate from JSON files."""
        qrels = self.load_qrels(qrels_file)
        runs = self.load_runs(runs_file)

        query_texts = None
        if query_texts_file:
            query_texts = self.load_query_texts(query_texts_file)

        return self.evaluate(qrels, runs, query_texts)

    def load_qrels(self, filepath: str) -> Dict[str, Dict[str, int]]:
        """Load relevance judgments from JSON file."""
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)

    def load_runs(self, filepath: str) -> Dict[str, Dict[str, float]]:
        """Load search results from JSON file."""
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)

    def load_query_texts(self, filepath: str) -> Dict[str, str]:
        """Load query texts from JSON file."""
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)

    def save_results(
        self, results: Tuple[AggregateMetrics, List[EvaluationResult]], output_file: str
    ):
        """Save evaluation results to JSON file."""
        aggregate, per_query = results

        output = {
            "aggregate_metrics": aggregate.to_dict(),
            "per_query_results": [r.to_dict() for r in per_query],
        }

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

    def _dict_to_measures_format(
        self, data: Dict[str, Dict[str, Any]]
    ) -> List[Tuple[str, str, Any]]:
        """Convert nested dict to ir_measures format."""
        result = []
        for query_id, docs in data.items():
            for doc_id, value in docs.items():
                result.append((query_id, doc_id, value))
        return result

    def compare_systems(
        self,
        qrels: Dict[str, Dict[str, int]],
        runs_baseline: Dict[str, Dict[str, float]],
        runs_new: Dict[str, Dict[str, float]],
        baseline_name: str = "Baseline",
        new_name: str = "New System",
    ) -> Dict[str, Dict[str, Any]]:
        """Compare two search systems."""
        _, results_baseline = self.evaluate(qrels, runs_baseline)
        _, results_new = self.evaluate(qrels, runs_new)

        comparison = {}

        # Get all metric names
        metric_names = set()
        for r in results_baseline + results_new:
            metric_names.update(r.metrics.keys())

        for metric_name in metric_names:
            baseline_values = [r.metrics.get(metric_name, 0) for r in results_baseline]
            new_values = [r.metrics.get(metric_name, 0) for r in results_new]

            baseline_avg = statistics.mean(baseline_values) if baseline_values else 0
            new_avg = statistics.mean(new_values) if new_values else 0

            improvement = new_avg - baseline_avg
            improvement_pct = (
                (improvement / baseline_avg * 100) if baseline_avg > 0 else 0
            )

            comparison[metric_name] = {
                baseline_name: round(baseline_avg, 4),
                new_name: round(new_avg, 4),
                "improvement": round(improvement, 4),
                "improvement_pct": round(improvement_pct, 2),
            }

        return comparison


if __name__ == "__main__":
    print("Boogle Evaluator - run 'uv run boogle eval' to use")
