#!/usr/bin/env python3
"""
Compare results across multiple model runs.

Reads run metadata and evaluation logs from runs/<model>/ directories and
produces a structured comparison report.

Usage:
    python compare_runs.py                        # compare all runs
    python compare_runs.py --runs gpt-5.1_rules claude-opus-4.6_rules
    python compare_runs.py --format markdown       # output as markdown table
    python compare_runs.py --export results.csv    # export to CSV
"""

import argparse
import json
import os
import sys
from collections import defaultdict
from pathlib import Path
from datetime import datetime

SCRIPT_DIR = Path(__file__).parent
# Runs are stored at the project root level (../../runs from baselines/promptSQL++)
RUNS_DIR = SCRIPT_DIR.parent.parent / "runs"


def load_run_meta(run_dir: Path) -> dict:
    """Load run metadata from run_meta.json."""
    meta_path = run_dir / "run_meta.json"
    if meta_path.exists():
        with open(meta_path) as f:
            return json.load(f)
    return {}


def load_eval_log(run_dir: Path) -> list[dict]:
    """Load evaluation JSONL log."""
    log_path = run_dir / "logs" / "log_sqlpp.jsonl"
    entries = []
    if log_path.exists():
        with open(log_path) as f:
            for line in f:
                line = line.strip()
                if line:
                    entries.append(json.loads(line))
    return entries


def extract_instance_results(entries: list[dict]) -> dict:
    """Extract per-instance results from log entries."""
    results = {}
    for entry in entries:
        if entry.get("event") == "result":
            iid = entry.get("instance_id")
            if iid:
                results[iid] = {
                    "score": entry.get("score", 0),
                    "error_info": entry.get("error_info"),
                }
        elif entry.get("event") == "query_error":
            iid = entry.get("instance_id")
            if iid and iid not in results:
                results[iid] = {
                    "score": 0,
                    "error_info": entry.get("error_message", "query_error"),
                }
    return results


def extract_scores(entries: list[dict]) -> dict:
    """Extract final/real scores from log entries."""
    scores = {}
    for entry in entries:
        if entry.get("event") == "final_score":
            scores["final_score"] = entry.get("score", 0)
            scores["correct"] = entry.get("correct", 0)
            scores["total_evaluated"] = entry.get("total_evaluated", 0)
        elif entry.get("event") == "real_score":
            scores["real_score"] = entry.get("score", 0)
            scores["total_local"] = entry.get("total_local", 0)
    return scores


def categorize_errors(results: dict) -> dict:
    """Categorize errors by type."""
    categories = defaultdict(int)
    for iid, res in results.items():
        if res["score"] == 0 and res.get("error_info"):
            err = res["error_info"]
            if "N1QL" in err or "syntax" in err.lower():
                categories["Syntax Error"] += 1
            elif "timeout" in err.lower() or "Timeout" in err:
                categories["Timeout"] += 1
            elif "Result Error" in err:
                categories["Wrong Result"] += 1
            elif "not found" in err.lower() or "missing" in err.lower():
                categories["Missing Data"] += 1
            else:
                categories["Other Error"] += 1
        elif res["score"] == 0:
            categories["Unknown Failure"] += 1
    return dict(categories)


def load_all_runs(run_names: list[str] | None = None) -> list[dict]:
    """Load all runs (or specified ones)."""
    if not RUNS_DIR.exists():
        print(f"No runs directory found at {RUNS_DIR}")
        sys.exit(1)

    run_dirs = sorted(RUNS_DIR.iterdir())
    if run_names:
        run_dirs = [RUNS_DIR / name for name in run_names if (RUNS_DIR / name).exists()]

    runs = []
    for run_dir in run_dirs:
        if not run_dir.is_dir():
            continue

        meta = load_run_meta(run_dir)
        entries = load_eval_log(run_dir)
        instance_results = extract_instance_results(entries)
        scores = extract_scores(entries)
        error_cats = categorize_errors(instance_results)

        runs.append({
            "name": run_dir.name,
            "dir": str(run_dir),
            "meta": meta,
            "scores": scores,
            "instance_results": instance_results,
            "error_categories": error_cats,
        })

    return runs


def print_summary_table(runs: list[dict], format: str = "text"):
    """Print a summary comparison table."""
    if not runs:
        print("No runs found.")
        return

    if format == "markdown":
        _print_markdown_table(runs)
    else:
        _print_text_table(runs)


def _print_text_table(runs: list[dict]):
    """Print ASCII table comparison."""
    # Header
    col_width = max(24, max(len(r["name"]) for r in runs) + 2)
    header_cols = ["Model"] + [r["name"] for r in runs]
    sep = "+" + "+".join("-" * (col_width + 2) for _ in header_cols) + "+"

    def row(label, values):
        cells = [f" {label:<{col_width}} "] + [f" {str(v):>{col_width}} " for v in values]
        return "|" + "|".join(cells) + "|"

    print("\n" + "=" * 60)
    print("  MODEL COMPARISON — PromptSQL++ Benchmark")
    print("=" * 60)

    print(sep)
    print(row("Model", [r["name"] for r in runs]))
    print(sep)

    # Model info
    print(row("Provider", [r["meta"].get("provider", "?") for r in runs]))
    print(row("Model", [r["meta"].get("model", "?") for r in runs]))
    print(row("Prompt Mode", [r["meta"].get("prompt_mode", "?") for r in runs]))
    print(sep)

    # Scores
    print(row("Correct", [r["scores"].get("correct", "?") for r in runs]))
    print(row("Total Evaluated", [r["scores"].get("total_evaluated", "?") for r in runs]))
    print(row("Total Local (135)", [r["scores"].get("total_local", "?") for r in runs]))
    print(row("Final Score",
              [f"{r['scores'].get('final_score', 0):.2%}" if isinstance(r['scores'].get('final_score'), (int, float)) else "?" for r in runs]))
    print(row("Real Score",
              [f"{r['scores'].get('real_score', 0):.2%}" if isinstance(r['scores'].get('real_score'), (int, float)) else "?" for r in runs]))
    print(sep)

    # Error breakdown
    all_error_types = set()
    for r in runs:
        all_error_types.update(r["error_categories"].keys())

    if all_error_types:
        print(row("--- Errors ---", ["" for _ in runs]))
        for err_type in sorted(all_error_types):
            print(row(f"  {err_type}", [r["error_categories"].get(err_type, 0) for r in runs]))
        print(sep)

    # Timestamps
    print(row("Started", [r["meta"].get("started_at", "?")[:19] for r in runs]))
    print(row("Completed", [r["meta"].get("completed_at", "?")[:19] if r["meta"].get("completed_at") else "?" for r in runs]))
    print(sep)
    print()


def _print_markdown_table(runs: list[dict]):
    """Print Markdown table comparison."""
    print("\n## Model Comparison — PromptSQL++ LLMS Comparision\n")

    headers = ["Model"] + [f"**{r['name']}**" for r in runs]
    print("| " + " | ".join(headers) + " |")
    print("| " + " | ".join(["---"] * len(headers)) + " |")

    def md_row(label, values):
        print(f"| {label} | " + " | ".join(str(v) for v in values) + " |")

    md_row("Provider", [r["meta"].get("provider", "?") for r in runs])
    md_row("Prompt Mode", [r["meta"].get("prompt_mode", "?") for r in runs])
    md_row("**Correct**", [f"**{r['scores'].get('correct', '?')}**" for r in runs])
    md_row("Total Evaluated", [r["scores"].get("total_evaluated", "?") for r in runs])
    md_row("Total Local", [r["scores"].get("total_local", "?") for r in runs])
    md_row("**Final Score**",
           [f"**{r['scores'].get('final_score', 0):.2%}**" if isinstance(r['scores'].get('final_score'), (int, float)) else "?" for r in runs])
    md_row("**Real Score**",
           [f"**{r['scores'].get('real_score', 0):.2%}**" if isinstance(r['scores'].get('real_score'), (int, float)) else "?" for r in runs])

    # Error breakdown
    all_error_types = set()
    for r in runs:
        all_error_types.update(r["error_categories"].keys())
    if all_error_types:
        md_row("", ["" for _ in runs])
        md_row("**Error Breakdown**", ["" for _ in runs])
        for err_type in sorted(all_error_types):
            md_row(f"  {err_type}", [r["error_categories"].get(err_type, 0) for r in runs])

    print()


def print_per_instance_diff(runs: list[dict]):
    """Print per-instance comparison showing where models differ."""
    if len(runs) < 2:
        print("Need at least 2 runs to compare per-instance differences.\n")
        return

    # Collect all instance IDs
    all_ids = set()
    for r in runs:
        all_ids.update(r["instance_results"].keys())

    # Find instances where models disagree
    disagreements = []
    for iid in sorted(all_ids):
        scores = []
        for r in runs:
            res = r["instance_results"].get(iid)
            scores.append(res["score"] if res else None)

        # Check if there's disagreement (different scores across models)
        valid_scores = [s for s in scores if s is not None]
        if len(set(valid_scores)) > 1:
            row = {"instance_id": iid}
            for i, r in enumerate(runs):
                res = r["instance_results"].get(iid)
                if res:
                    row[r["name"]] = "✅" if res["score"] == 1 else f"❌ {res.get('error_info', '')[:40]}"
                else:
                    row[r["name"]] = "—"
            disagreements.append(row)

    if not disagreements:
        print("All models agree on every instance.\n")
        return

    print(f"\n{'=' * 60}")
    print(f"  Per-Instance Differences ({len(disagreements)} instances)")
    print(f"{'=' * 60}\n")

    # Print as a simple table
    run_names = [r["name"] for r in runs]
    col_width = max(20, max(len(n) for n in run_names) + 2)

    header = f"{'Instance ID':<16} | " + " | ".join(f"{n:<{col_width}}" for n in run_names)
    print(header)
    print("-" * len(header))

    for row in disagreements:
        cells = [f"{row['instance_id']:<16}"]
        for name in run_names:
            cells.append(f"{row.get(name, '—'):<{col_width}}")
        print(" | ".join(cells))

    print()

    # Print summary: unique wins per model
    print("Unique correct answers per model:")
    for r in runs:
        unique_correct = set()
        other_runs = [o for o in runs if o["name"] != r["name"]]
        for iid in all_ids:
            my_res = r["instance_results"].get(iid)
            if my_res and my_res["score"] == 1:
                others_wrong = all(
                    o["instance_results"].get(iid, {}).get("score", 0) == 0
                    for o in other_runs
                )
                if others_wrong:
                    unique_correct.add(iid)
        if unique_correct:
            print(f"  {r['name']}: {len(unique_correct)} unique — {', '.join(sorted(unique_correct)[:10])}")
        else:
            print(f"  {r['name']}: 0 unique")
    print()


def export_csv(runs: list[dict], output_path: str):
    """Export per-instance results to CSV for external analysis."""
    try:
        import pandas as pd
    except ImportError:
        print("pandas required for CSV export. Install with: pip install pandas")
        return

    all_ids = set()
    for r in runs:
        all_ids.update(r["instance_results"].keys())

    rows = []
    for iid in sorted(all_ids):
        row = {"instance_id": iid}
        for r in runs:
            res = r["instance_results"].get(iid)
            prefix = r["name"]
            if res:
                row[f"{prefix}_score"] = res["score"]
                row[f"{prefix}_error"] = res.get("error_info", "")
            else:
                row[f"{prefix}_score"] = None
                row[f"{prefix}_error"] = "not evaluated"
        rows.append(row)

    df = pd.DataFrame(rows)
    df.to_csv(output_path, index=False)
    print(f"✅ Exported comparison to: {output_path}")
    print(f"   {len(rows)} instances × {len(runs)} models\n")


def main():
    parser = argparse.ArgumentParser(description="Compare PromptSQL++ model runs")
    parser.add_argument("--runs", nargs="*", default=None,
                        help="Specific run names to compare (default: all)")
    parser.add_argument("--format", choices=["text", "markdown"], default="text",
                        help="Output table format")
    parser.add_argument("--export", type=str, default=None,
                        help="Export per-instance results to CSV")
    parser.add_argument("--diff", action="store_true",
                        help="Show per-instance differences between models")
    parser.add_argument("--list", action="store_true",
                        help="Just list available runs")
    args = parser.parse_args()

    runs = load_all_runs(args.runs)

    if args.list or not runs:
        if not runs:
            print("No runs found. Run the pipeline first with: ./run.sh")
        else:
            print(f"\nAvailable runs ({len(runs)}):\n")
            for r in runs:
                score = r["scores"].get("real_score")
                score_str = f"{score:.2%}" if isinstance(score, (int, float)) else "not evaluated"
                print(f"  • {r['name']:<30} score={score_str}  ({r['meta'].get('model', '?')})")
            print()
        return

    # Print summary table
    print_summary_table(runs, format=args.format)

    # Print per-instance diff
    if args.diff and len(runs) >= 2:
        print_per_instance_diff(runs)

    # Export CSV
    if args.export:
        export_csv(runs, args.export)


if __name__ == "__main__":
    main()
