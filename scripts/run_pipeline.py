#!/usr/bin/env python3
"""Run full DKB pipeline on collected data."""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from dkb_runtime.core.config import get_settings
from dkb_runtime.models import CanonicalDirective, DimensionModel, RawDirective, SourceSnapshot
from dkb_runtime.services.canonicalizer import CanonicalResult, canonicalize
from dkb_runtime.services.extractor import extract_directives
from dkb_runtime.services.scoring import score_directive
from dkb_runtime.services.verdict import evaluate_directive

STAGE_ORDER = ("extract", "canonicalize", "score", "verdict")


@dataclass
class StageStats:
    name: str
    seconds: float
    detail: str


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--stage",
        action="append",
        choices=list(STAGE_ORDER),
        help="Run only these stages, in pipeline order (repeatable). Default: all stages.",
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        help="Print per-directive lines during score and verdict (legacy verbose output).",
    )
    return p.parse_args(argv)


def _resolve_stages(requested: list[str] | None) -> tuple[str, ...]:
    if not requested:
        return STAGE_ORDER
    seen: set[str] = set()
    ordered: list[str] = []
    for s in STAGE_ORDER:
        if s in requested:
            if s not in seen:
                seen.add(s)
                ordered.append(s)
    return tuple(ordered)


def _raw_ids_for_captured_snapshots(db) -> list[UUID]:
    stmt = (
        select(RawDirective.raw_directive_id)
        .join(SourceSnapshot, RawDirective.snapshot_id == SourceSnapshot.snapshot_id)
        .where(SourceSnapshot.capture_status == "captured")
    )
    return list(db.scalars(stmt).all())


def _load_active_canonicals(db) -> list[CanonicalResult]:
    rows = db.scalars(
        select(CanonicalDirective).where(CanonicalDirective.status == "active").order_by(CanonicalDirective.preferred_name)
    ).all()
    return [
        CanonicalResult(directive_id=r.directive_id, preferred_name=r.preferred_name, mapped_raw_count=0)
        for r in rows
    ]


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    stages = _resolve_stages(args.stage)
    settings = get_settings()
    engine = create_engine(settings.database_url, future=True)
    SessionLocal = sessionmaker(bind=engine)
    db = SessionLocal()

    stage_stats: list[StageStats] = []
    all_raw_ids: list[UUID] = []
    canonical_results: list[CanonicalResult] = []

    try:
        dimension_model = db.scalars(
            select(DimensionModel).where(DimensionModel.is_active.is_(True))
        ).first()
        if "score" in stages or "verdict" in stages:
            if not dimension_model:
                print("ERROR: No active dimension model found. Run seed first.", file=sys.stderr)
                return

        print(f"Stages: {' -> '.join(stages)}\n")

        if "extract" in stages:
            t0 = time.perf_counter()
            print("=== Stage: extract ===")
            snapshots = db.scalars(
                select(SourceSnapshot)
                .where(SourceSnapshot.capture_status == "captured")
                .order_by(SourceSnapshot.captured_at.desc())
            ).all()
            n_snap = len(snapshots)
            print(f"Found {n_snap} captured snapshots to extract from")
            for i, snapshot in enumerate(snapshots, start=1):
                print(f"  Extracting snapshot [{i}/{n_snap}] {snapshot.snapshot_id} ...")
                results = extract_directives(db, snapshot.snapshot_id)
                all_raw_ids.extend(r.raw_directive_id for r in results)
                print(f"    extracted {len(results)} raw directives (running total raw: {len(all_raw_ids)})")
            elapsed = time.perf_counter() - t0
            stage_stats.append(
                StageStats("extract", elapsed, f"{n_snap} snapshots, {len(all_raw_ids)} raw directive rows")
            )
            print(f"  (extract done in {elapsed:.2f}s)\n")

        if "canonicalize" in stages:
            t0 = time.perf_counter()
            print("=== Stage: canonicalize ===")
            raw_ids = list(all_raw_ids) if all_raw_ids else _raw_ids_for_captured_snapshots(db)
            if not all_raw_ids:
                all_raw_ids = raw_ids
            print(f"Canonicalizing {len(raw_ids)} raw directives ...")
            canonical_results = canonicalize(db, raw_ids)
            print(f"  created/updated {len(canonical_results)} canonical directives")
            elapsed = time.perf_counter() - t0
            stage_stats.append(
                StageStats("canonicalize", elapsed, f"{len(canonical_results)} canonical directives")
            )
            print(f"  (canonicalize done in {elapsed:.2f}s)\n")

        if "score" in stages:
            t0 = time.perf_counter()
            print("=== Stage: score ===")
            targets = canonical_results if canonical_results else _load_active_canonicals(db)
            n = len(targets)
            print(f"Scoring {n} canonical directives ...")
            for i, cr in enumerate(targets, start=1):
                if args.verbose:
                    print(f"\n  --- Scoring: {cr.preferred_name} ---")
                elif i == 1 or i == n or i % 25 == 0:
                    print(f"  scoring [{i}/{n}] ...")
                scores = score_directive(db, cr.directive_id, dimension_model.dimension_model_id)
                if args.verbose:
                    print(f"    scored {len(scores)} dimensions")
            elapsed = time.perf_counter() - t0
            stage_stats.append(StageStats("score", elapsed, f"{n} directives"))
            print(f"  (score done in {elapsed:.2f}s)\n")

        if "verdict" in stages:
            t0 = time.perf_counter()
            print("=== Stage: verdict ===")
            targets = canonical_results if canonical_results else _load_active_canonicals(db)
            n = len(targets)
            print(f"Evaluating verdicts for {n} canonical directives ...")
            for i, cr in enumerate(targets, start=1):
                if args.verbose:
                    print(f"\n  --- Evaluating: {cr.preferred_name} ---")
                elif i == 1 or i == n or i % 25 == 0:
                    print(f"  verdict [{i}/{n}] ...")
                verdict = evaluate_directive(db, cr.directive_id)
                if args.verbose:
                    print(f"    Verdict: trust={verdict.trust_state}, rec={verdict.recommendation_state}")
            elapsed = time.perf_counter() - t0
            stage_stats.append(StageStats("verdict", elapsed, f"{n} directives"))
            print(f"  (verdict done in {elapsed:.2f}s)\n")

        total_s = sum(s.seconds for s in stage_stats)
        print("=== Pipeline summary ===")
        for s in stage_stats:
            print(f"  {s.name:14} {s.seconds:8.2f}s  ({s.detail})")
        print(f"  {'total':14} {total_s:8.2f}s")
        if "extract" in stages:
            print(f"  raw directives (this run / extract path): {len(all_raw_ids)}")
        if "canonicalize" in stages:
            print(f"  canonical directives: {len(canonical_results)}")
        print("\n=== Pipeline complete ===")

    finally:
        db.close()


if __name__ == "__main__":
    main()
