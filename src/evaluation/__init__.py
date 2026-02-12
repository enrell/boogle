"""
Boogle Evaluation System

Comprehensive evaluation system for search quality using ir_measures.

Usage:
    from src.evaluation import BoogleEvaluator

    evaluator = BoogleEvaluator()
    results = evaluator.evaluate_from_files(
        qrels_file="data/evaluation/qrels.json",
        runs_file="data/evaluation/runs.json"
    )

    print(results)
"""

from .evaluator import BoogleEvaluator, RelevanceGrade
from .dataset import EvaluationDataset, QuerySample
from .annotator import RelevanceAnnotator
from .reporter import EvaluationReporter

__all__ = [
    "BoogleEvaluator",
    "RelevanceGrade",
    "EvaluationDataset",
    "QuerySample",
    "RelevanceAnnotator",
    "EvaluationReporter",
]
