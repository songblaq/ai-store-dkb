#!/usr/bin/env python3
"""Summarize catalog.json: print stats and write storage/reports/summary.json."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any


def load_catalog(path: Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def build_summary(catalog: dict[str, Any]) -> dict[str, Any]:
    directives = catalog.get("directives") or []
    by_category: Counter[str] = Counter()
    verdict_counts: Counter[str] = Counter()
    overall_scores: list[float] = []

    dim_values: dict[str, list[float]] = {}

    for d in directives:
        cat = d.get("category") or "_uncategorized"
        by_category[cat] += 1
        v = d.get("verdict")
        if isinstance(v, dict):
            verdict_counts[str(v.get("recommendation") or "unknown")] += 1
        else:
            verdict_counts[str(v or "unknown")] += 1
        os_ = d.get("overall_score")
        if isinstance(os_, (int, float)):
            overall_scores.append(float(os_))

        scores = d.get("scores") or {}
        for key, info in scores.items():
            if isinstance(info, dict) and "score" in info:
                dim_values.setdefault(key, []).append(float(info["score"]))

    def dist(vals: list[float]) -> dict[str, float]:
        if not vals:
            return {"min": 0.0, "max": 0.0, "avg": 0.0, "count": 0}
        return {
            "min": round(min(vals), 4),
            "max": round(max(vals), 4),
            "avg": round(sum(vals) / len(vals), 4),
            "count": len(vals),
        }

    score_distribution = {k: dist(v) for k, v in sorted(dim_values.items())}
    overall_dist = dist(overall_scores)

    top_10 = sorted(
        directives,
        key=lambda x: float(x.get("overall_score") or 0.0),
        reverse=True,
    )[:10]
    top_10_out = [
        {
            "preferred_name": x.get("preferred_name"),
            "category": x.get("category"),
            "overall_score": x.get("overall_score"),
            "verdict": (x.get("verdict") or {}).get("recommendation")
            if isinstance(x.get("verdict"), dict)
            else x.get("verdict"),
        }
        for x in top_10
    ]

    return {
        "catalog_version": catalog.get("version"),
        "build_id": catalog.get("build_id"),
        "timestamp": catalog.get("timestamp"),
        "directive_total": len(directives),
        "count_by_category": dict(sorted(by_category.items())),
        "verdict_counts": dict(sorted(verdict_counts.items())),
        "overall_score_distribution": overall_dist,
        "score_distribution_by_dimension": score_distribution,
        "top_10_by_overall_score": top_10_out,
    }


def print_summary(s: dict[str, Any]) -> None:
    print("=== Catalog ===")
    print(f"  version: {s.get('catalog_version')}  build_id: {s.get('build_id')}")
    print(f"  directives: {s.get('directive_total')}")

    print("\n=== Count by category ===")
    for k, v in (s.get("count_by_category") or {}).items():
        print(f"  {k}: {v}")

    print("\n=== Verdict counts ===")
    for k, v in (s.get("verdict_counts") or {}).items():
        print(f"  {k}: {v}")

    od = s.get("overall_score_distribution") or {}
    print("\n=== Overall score distribution ===")
    print(f"  min={od.get('min')} max={od.get('max')} avg={od.get('avg')} (n={od.get('count')})")

    print("\n=== Score distribution by dimension ===")
    for dim, stats in sorted((s.get("score_distribution_by_dimension") or {}).items()):
        print(
            f"  {dim}: min={stats['min']} max={stats['max']} "
            f"avg={stats['avg']} (n={stats['count']})"
        )

    print("\n=== Top 10 by overall score ===")
    for i, row in enumerate(s.get("top_10_by_overall_score") or [], start=1):
        print(
            f"  {i:2}. {row.get('overall_score')}  {row.get('preferred_name')} "
            f"({row.get('category')}) [{row.get('verdict')}]"
        )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    root = Path(__file__).resolve().parent.parent
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--catalog",
        type=Path,
        default=root / "dist" / "catalog" / "catalog.json",
    )
    p.add_argument(
        "--output",
        type=Path,
        default=root / "storage" / "reports" / "summary.json",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if not args.catalog.is_file():
        print(f"ERROR: catalog not found: {args.catalog}", file=sys.stderr)
        return 1

    catalog = load_catalog(args.catalog)
    summary = build_summary(catalog)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print_summary(summary)
    print(f"\nWrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
