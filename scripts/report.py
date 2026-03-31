#!/usr/bin/env python3
"""Summarize catalog contents: sources, directives, scores, verdicts."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from sqlalchemy import and_, create_engine, func, select
from sqlalchemy.orm import sessionmaker

from dkb_runtime.core.config import get_settings
from dkb_runtime.models import (
    CanonicalDirective,
    DimensionScore,
    RawDirective,
    Source,
    SourceSnapshot,
    Verdict,
)


def load_sources_config(path: Path) -> dict[str, Any]:
    with open(path) as f:
        return json.load(f)


def origin_uri_to_category(sources_config: dict[str, Any]) -> dict[str, str]:
    """Map origin_uri -> category name from config (dict or list shape)."""
    out: dict[str, str] = {}
    categories = sources_config.get("categories", [])
    if isinstance(categories, list):
        for category in categories:
            name = category["name"]
            for source_def in category.get("sources", []):
                uri = source_def.get("origin_uri") or source_def.get("url")
                if uri:
                    out[uri] = name
    elif isinstance(categories, dict):
        for category_name, source_list in categories.items():
            for source_def in source_list:
                uri = source_def.get("origin_uri") or source_def.get("url")
                if uri:
                    out[uri] = category_name
    return out


def _latest_verdict_subquery():
    return (
        select(Verdict.directive_id.label("directive_id"), func.max(Verdict.evaluated_at).label("mx"))
        .group_by(Verdict.directive_id)
        .subquery()
    )


def build_report(db, sources_config_path: Path) -> dict[str, Any]:
    cfg = load_sources_config(sources_config_path)
    uri_cat = origin_uri_to_category(cfg)

    sources = db.scalars(select(Source)).all()
    by_category: Counter[str] = Counter()
    for s in sources:
        by_category[uri_cat.get(s.origin_uri, "_uncategorized")] += 1

    rd_stmt = (
        select(RawDirective.declared_type, func.count())
        .join(SourceSnapshot, RawDirective.snapshot_id == SourceSnapshot.snapshot_id)
        .where(SourceSnapshot.capture_status == "captured")
        .group_by(RawDirective.declared_type)
    )
    by_declared_type = {row[0]: int(row[1]) for row in db.execute(rd_stmt).all()}

    score_stmt = select(
        DimensionScore.dimension_group,
        func.avg(DimensionScore.score),
        func.min(DimensionScore.score),
        func.max(DimensionScore.score),
        func.count(),
    ).group_by(DimensionScore.dimension_group)
    score_by_group: dict[str, dict[str, float | int]] = {}
    for row in db.execute(score_stmt).all():
        grp, avg_s, min_s, max_s, cnt = row
        score_by_group[grp or ""] = {
            "avg": float(avg_s) if avg_s is not None else 0.0,
            "min": float(min_s) if min_s is not None else 0.0,
            "max": float(max_s) if max_s is not None else 0.0,
            "count": int(cnt),
        }

    mx = _latest_verdict_subquery()
    v_alias = Verdict
    trust_stmt = (
        select(v_alias.trust_state, func.count())
        .join(mx, and_(v_alias.directive_id == mx.c.directive_id, v_alias.evaluated_at == mx.c.mx))
        .group_by(v_alias.trust_state)
    )
    rec_stmt = (
        select(v_alias.recommendation_state, func.count())
        .join(mx, and_(v_alias.directive_id == mx.c.directive_id, v_alias.evaluated_at == mx.c.mx))
        .group_by(v_alias.recommendation_state)
    )
    verdict_trust = {row[0]: int(row[1]) for row in db.execute(trust_stmt).all()}
    verdict_rec = {row[0]: int(row[1]) for row in db.execute(rec_stmt).all()}

    avg_sub = (
        select(
            DimensionScore.directive_id.label("directive_id"),
            func.avg(DimensionScore.score).label("avg_score"),
        )
        .group_by(DimensionScore.directive_id)
        .subquery()
    )
    top_stmt = (
        select(CanonicalDirective.preferred_name, avg_sub.c.avg_score)
        .join(avg_sub, avg_sub.c.directive_id == CanonicalDirective.directive_id)
        .where(CanonicalDirective.status == "active")
        .order_by(avg_sub.c.avg_score.desc())
        .limit(10)
    )
    top_directives = [
        {"preferred_name": row[0], "avg_score": float(row[1])} for row in db.execute(top_stmt).all()
    ]

    return {
        "source_count_by_category": dict(sorted(by_category.items())),
        "directive_count_by_declared_type": dict(sorted(by_declared_type.items(), key=lambda x: (-x[1], x[0]))),
        "score_distribution_by_dimension_group": score_by_group,
        "verdict_distribution": {
            "trust_state": dict(sorted(verdict_trust.items())),
            "recommendation_state": dict(sorted(verdict_rec.items())),
        },
        "top_scored_directives": top_directives,
    }


def print_text_report(data: dict[str, Any]) -> None:
    print("=== Source count by category ===")
    for k, v in data["source_count_by_category"].items():
        print(f"  {k}: {v}")

    print("\n=== Directive count by declared_type (captured snapshots) ===")
    for k, v in data["directive_count_by_declared_type"].items():
        print(f"  {k}: {v}")

    print("\n=== Score distribution by dimension_group ===")
    for grp, stats in sorted(data["score_distribution_by_dimension_group"].items()):
        label = grp or "(empty)"
        print(
            f"  {label}: avg={stats['avg']:.4f} min={stats['min']:.4f} "
            f"max={stats['max']:.4f} (n={stats['count']})"
        )

    vd = data["verdict_distribution"]
    print("\n=== Verdict distribution (latest per directive) ===")
    print("  trust_state:")
    for k, v in vd["trust_state"].items():
        print(f"    {k}: {v}")
    print("  recommendation_state:")
    for k, v in vd["recommendation_state"].items():
        print(f"    {k}: {v}")

    print("\n=== Top 10 directives by average dimension score ===")
    for i, row in enumerate(data["top_scored_directives"], start=1):
        print(f"  {i:2}. {row['avg_score']:.4f}  {row['preferred_name']}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--json",
        action="store_true",
        help="Emit report as JSON to stdout instead of plain text.",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    settings = get_settings()
    engine = create_engine(settings.database_url, future=True)
    SessionLocal = sessionmaker(bind=engine)
    db = SessionLocal()
    sources_path = Path(__file__).resolve().parent.parent / "config" / "sources.json"
    try:
        data = build_report(db, sources_path)
        if args.json:
            json.dump(data, sys.stdout, indent=2)
            sys.stdout.write("\n")
        else:
            print_text_report(data)
    finally:
        db.close()


if __name__ == "__main__":
    main()
