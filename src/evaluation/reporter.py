"""
Evaluation Reporter

Generates comprehensive evaluation reports with visualizations.
"""

import json
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

import matplotlib.pyplot as plt
import seaborn as sns


class EvaluationReporter:
    """
    Generate evaluation reports with visualizations.

    Usage:
        reporter = EvaluationReporter()

        # Generate report
        reporter.generate_report(
            results_file="data/evaluation/results.json",
            output_dir="reports/evaluation"
        )
    """

    def __init__(self):
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    def generate_report(
        self, results_file: str, output_dir: str, system_name: str = "Boogle"
    ):
        """
        Generate comprehensive evaluation report.

        Args:
            results_file: Path to evaluation results JSON
            output_dir: Directory for output files
            system_name: Name of the system being evaluated
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Load results
        with open(results_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        aggregate = data["aggregate_metrics"]
        per_query = data.get("per_query_results", [])

        # Generate markdown report
        self._generate_markdown_report(aggregate, per_query, output_path, system_name)

        # Generate visualizations
        self._generate_charts(aggregate, per_query, output_path)

        print(f"Report generated in: {output_path}")

    def _generate_markdown_report(
        self,
        aggregate: Dict,
        per_query: List[Dict],
        output_path: Path,
        system_name: str,
    ):
        """Generate markdown report."""

        report = f"""# {system_name} Evaluation Report

**Generated:** {self.timestamp}
**Number of Queries:** {aggregate.get("num_queries", "N/A")}

## Aggregate Metrics

| Metric | Value |
|--------|-------|
| nDCG@10 | {aggregate.get("nDCG@10", 0):.4f} |
| nDCG@20 | {aggregate.get("nDCG@20", 0):.4f} |
| MAP | {aggregate.get("MAP", 0):.4f} |
| P@5 | {aggregate.get("P@5", 0):.4f} |
| P@10 | {aggregate.get("P@10", 0):.4f} |
| P@20 | {aggregate.get("P@20", 0):.4f} |
| R@10 | {aggregate.get("R@10", 0):.4f} |
| R@20 | {aggregate.get("R@20", 0):.4f} |
| R@100 | {aggregate.get("R@100", 0):.4f} |
| MRR | {aggregate.get("MRR", 0):.4f} |

## Interpretation

- **nDCG@10**: {self._interpret_ndcg(aggregate.get("nDCG@10", 0))}
- **MAP**: {self._interpret_map(aggregate.get("MAP", 0))}
- **MRR**: {self._interpret_mrr(aggregate.get("MRR", 0))}

## Per-Query Results

"""

        # Add per-query table
        if per_query:
            report += "| Query | nDCG@10 | MAP | Relevant | Retrieved |\n"
            report += "|-------|---------|-----|----------|-----------|\n"

            for q in per_query[:20]:  # Show first 20
                query_text = q.get("query_text", q["query_id"])[:30]
                metrics = q.get("metrics", {})
                ndcg = metrics.get("nDCG(cutoff=10)", metrics.get("nDCG@10", 0))
                map_val = metrics.get("AP", metrics.get("MAP", 0))
                report += f"| {query_text} | {ndcg:.3f} | {map_val:.3f} | {q.get('num_relevant', 0)} | {q.get('num_retrieved', 0)} |\n"

        report += "\n## Recommendations\n\n"
        report += self._generate_recommendations(aggregate)

        # Write report
        report_file = output_path / f"report_{self.timestamp}.md"
        with open(report_file, "w", encoding="utf-8") as f:
            f.write(report)

        print(f"  Markdown report: {report_file}")

    def _generate_charts(
        self, aggregate: Dict, per_query: List[Dict], output_path: Path
    ):
        """Generate visualization charts."""

        # Set style
        sns.set_style("whitegrid")

        # 1. Metrics Overview
        fig, ax = plt.subplots(figsize=(10, 6))

        metrics = ["nDCG@10", "MAP", "P@10", "R@20", "MRR"]
        values = [
            aggregate.get("nDCG@10", 0),
            aggregate.get("MAP", 0),
            aggregate.get("P@10", 0),
            aggregate.get("R@20", 0),
            aggregate.get("MRR", 0),
        ]

        colors = plt.cm.RdYlGn([v for v in values])
        bars = ax.barh(metrics, values, color=colors)
        ax.set_xlim(0, 1)
        ax.set_xlabel("Score")
        ax.set_title("Boogle Search Quality Metrics")

        # Add value labels
        for bar, val in zip(bars, values):
            width = bar.get_width()
            ax.text(
                width + 0.01,
                bar.get_y() + bar.get_height() / 2,
                f"{val:.3f}",
                ha="left",
                va="center",
            )

        plt.tight_layout()
        chart_file = output_path / f"metrics_overview_{self.timestamp}.png"
        plt.savefig(chart_file, dpi=150, bbox_inches="tight")
        plt.close()
        print(f"  Chart: {chart_file}")

        # 2. Per-query distribution (if available)
        if per_query:
            self._generate_query_distribution_chart(per_query, output_path)

    def _generate_query_distribution_chart(
        self, per_query: List[Dict], output_path: Path
    ):
        """Generate per-query metric distribution."""

        fig, axes = plt.subplots(2, 2, figsize=(12, 10))

        # nDCG@10 distribution
        ndcg_values = [
            q.get("metrics", {}).get("nDCG(cutoff=10)", 0) for q in per_query
        ]
        axes[0, 0].hist(ndcg_values, bins=20, edgecolor="black")
        axes[0, 0].set_xlabel("nDCG@10")
        axes[0, 0].set_ylabel("Number of Queries")
        axes[0, 0].set_title("nDCG@10 Distribution")
        axes[0, 0].axvline(
            x=sum(ndcg_values) / len(ndcg_values),
            color="r",
            linestyle="--",
            label="Mean",
        )
        axes[0, 0].legend()

        # MAP distribution
        map_values = [q.get("metrics", {}).get("AP", 0) for q in per_query]
        axes[0, 1].hist(
            map_values, bins=20, edgecolor="black", color="green", alpha=0.7
        )
        axes[0, 1].set_xlabel("MAP")
        axes[0, 1].set_ylabel("Number of Queries")
        axes[0, 1].set_title("MAP Distribution")
        axes[0, 1].axvline(
            x=sum(map_values) / len(map_values), color="r", linestyle="--", label="Mean"
        )
        axes[0, 1].legend()

        # Precision@10 distribution
        p10_values = [q.get("metrics", {}).get("P(cutoff=10)", 0) for q in per_query]
        axes[1, 0].hist(
            p10_values, bins=20, edgecolor="black", color="orange", alpha=0.7
        )
        axes[1, 0].set_xlabel("P@10")
        axes[1, 0].set_ylabel("Number of Queries")
        axes[1, 0].set_title("Precision@10 Distribution")
        axes[1, 0].axvline(
            x=sum(p10_values) / len(p10_values), color="r", linestyle="--", label="Mean"
        )
        axes[1, 0].legend()

        # Recall@20 distribution
        r20_values = [q.get("metrics", {}).get("R(cutoff=20)", 0) for q in per_query]
        axes[1, 1].hist(
            r20_values, bins=20, edgecolor="black", color="purple", alpha=0.7
        )
        axes[1, 1].set_xlabel("R@20")
        axes[1, 1].set_ylabel("Number of Queries")
        axes[1, 1].set_title("Recall@20 Distribution")
        axes[1, 1].axvline(
            x=sum(r20_values) / len(r20_values), color="r", linestyle="--", label="Mean"
        )
        axes[1, 1].legend()

        plt.tight_layout()
        chart_file = output_path / f"query_distribution_{self.timestamp}.png"
        plt.savefig(chart_file, dpi=150, bbox_inches="tight")
        plt.close()
        print(f"  Chart: {chart_file}")

    def _interpret_ndcg(self, value: float) -> str:
        """Interpret nDCG score."""
        if value >= 0.8:
            return "Excellent ranking quality"
        elif value >= 0.6:
            return "Good ranking quality"
        elif value >= 0.4:
            return "Fair ranking quality"
        else:
            return "Poor ranking quality - needs improvement"

    def _interpret_map(self, value: float) -> str:
        """Interpret MAP score."""
        if value >= 0.7:
            return "Very high precision across all ranks"
        elif value >= 0.5:
            return "Good precision across ranks"
        elif value >= 0.3:
            return "Moderate precision"
        else:
            return "Low precision - consider query processing improvements"

    def _interpret_mrr(self, value: float) -> str:
        """Interpret MRR score."""
        if value >= 0.8:
            return "First relevant result usually in top 1-2 positions"
        elif value >= 0.5:
            return "First relevant result usually in top 2-3 positions"
        else:
            return "First relevant result may be lower in ranking"

    def _generate_recommendations(self, aggregate: Dict) -> str:
        """Generate improvement recommendations."""
        recommendations = []

        ndcg = aggregate.get("nDCG@10", 0)
        map_val = aggregate.get("MAP", 0)
        p10 = aggregate.get("P@10", 0)
        r20 = aggregate.get("R@20", 0)

        if ndcg < 0.5:
            recommendations.append(
                "- **Ranking Quality**: Consider implementing BM25 parameter tuning (k1, b)"
            )

        if map_val < 0.4:
            recommendations.append(
                "- **Precision**: Add query expansion with synonyms to improve recall"
            )

        if p10 < 0.5:
            recommendations.append(
                "- **Top Results**: Review semantic chunking - ensure chapters aren't split mid-sentence"
            )

        if r20 < 0.3:
            recommendations.append(
                "- **Recall**: Consider lowering BM25 minimum score threshold"
            )

        if not recommendations:
            recommendations.append(
                "- **Status**: System is performing well! Continue monitoring for regressions."
            )

        return "\n".join(recommendations)


if __name__ == "__main__":
    print("Evaluation Reporter - run 'uv run boogle eval-report' to generate reports")
