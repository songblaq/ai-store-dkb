#!/usr/bin/env python3
"""Run full DKB pipeline on collected data."""

from __future__ import annotations

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from dkb_runtime.core.config import get_settings
from dkb_runtime.models import DimensionModel, SourceSnapshot
from dkb_runtime.services.canonicalizer import canonicalize
from dkb_runtime.services.extractor import extract_directives
from dkb_runtime.services.scoring import score_directive
from dkb_runtime.services.verdict import evaluate_directive


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, future=True)
    SessionLocal = sessionmaker(bind=engine)
    db = SessionLocal()

    try:
        snapshots = db.scalars(
            select(SourceSnapshot)
            .where(SourceSnapshot.capture_status == "captured")
            .order_by(SourceSnapshot.captured_at.desc())
        ).all()
        print(f"Found {len(snapshots)} snapshots to process")

        all_raw_ids: list = []
        for snapshot in snapshots:
            print(f"\n--- Extracting: {snapshot.snapshot_id} ---")
            results = extract_directives(db, snapshot.snapshot_id)
            raw_ids = [r.raw_directive_id for r in results]
            all_raw_ids.extend(raw_ids)
            print(f"  Extracted {len(results)} raw directives")

        print(f"\n--- Canonicalizing {len(all_raw_ids)} raw directives ---")
        canonical_results = canonicalize(db, all_raw_ids)
        print(f"  Created {len(canonical_results)} canonical directives")

        dimension_model = db.scalars(
            select(DimensionModel).where(DimensionModel.is_active.is_(True))
        ).first()
        if not dimension_model:
            print("ERROR: No active dimension model found. Run seed first.")
            return

        for cr in canonical_results:
            print(f"\n--- Scoring: {cr.preferred_name} ---")
            scores = score_directive(db, cr.directive_id, dimension_model.dimension_model_id)
            print(f"  Scored {len(scores)} dimensions")

        for cr in canonical_results:
            print(f"\n--- Evaluating: {cr.preferred_name} ---")
            verdict = evaluate_directive(db, cr.directive_id)
            print(f"  Verdict: trust={verdict.trust_state}, rec={verdict.recommendation_state}")

        print("\n=== Pipeline complete ===")
        print(f"Snapshots: {len(snapshots)}")
        print(f"Raw directives: {len(all_raw_ids)}")
        print(f"Canonical directives: {len(canonical_results)}")

    finally:
        db.close()


if __name__ == "__main__":
    main()
