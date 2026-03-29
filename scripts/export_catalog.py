#!/usr/bin/env python3
"""Export catalog to dist/catalog/."""

from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from dkb_runtime.core.config import get_settings
from dkb_runtime.models import CanonicalDirective, DimensionScore, Verdict


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, future=True)
    SessionLocal = sessionmaker(bind=engine)
    db = SessionLocal()

    output_dir = Path(__file__).resolve().parent.parent / "dist" / "catalog"
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        directives = db.scalars(
            select(CanonicalDirective)
            .where(CanonicalDirective.status == "active")
            .order_by(CanonicalDirective.preferred_name)
        ).all()

        catalog = {
            "version": "0.1.0",
            "instance": "ai-store",
            "directive_count": len(directives),
            "directives": [],
        }

        for d in directives:
            scores = db.scalars(
                select(DimensionScore).where(DimensionScore.directive_id == d.directive_id)
            ).all()
            verdict = db.scalars(
                select(Verdict)
                .where(Verdict.directive_id == d.directive_id)
                .order_by(Verdict.evaluated_at.desc())
            ).first()

            entry = {
                "directive_id": str(d.directive_id),
                "preferred_name": d.preferred_name,
                "normalized_summary": d.normalized_summary,
                "status": d.status,
                "scores": {
                    s.dimension_key: {"score": s.score, "confidence": s.confidence} for s in scores
                },
                "verdict": {
                    "provenance": verdict.provenance_state,
                    "trust": verdict.trust_state,
                    "legal": verdict.legal_state,
                    "lifecycle": verdict.lifecycle_state,
                    "recommendation": verdict.recommendation_state,
                }
                if verdict
                else None,
            }
            catalog["directives"].append(entry)

        catalog_path = output_dir / "catalog.json"
        with open(catalog_path, "w") as f:
            json.dump(catalog, f, indent=2)

        print(f"Exported {len(directives)} directives to {catalog_path}")

    finally:
        db.close()


if __name__ == "__main__":
    main()
