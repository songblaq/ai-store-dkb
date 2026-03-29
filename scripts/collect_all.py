#!/usr/bin/env python3
"""Collect all configured sources using dkb-runtime services."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from dkb_runtime.core.config import get_settings
from dkb_runtime.models import Source
from dkb_runtime.services.collector import collect_source


def load_sources_config(path: Path) -> dict[str, Any]:
    with open(path) as f:
        return json.load(f)


def iter_category_sources(
    sources_config: dict[str, Any],
) -> list[tuple[str, dict[str, Any]]]:
    """Flatten (category_name, source_def) from repo sources.json shape."""
    out: list[tuple[str, dict[str, Any]]] = []
    categories = sources_config.get("categories", [])
    if isinstance(categories, list):
        for category in categories:
            category_name = category["name"]
            for source_def in category.get("sources", []):
                out.append((category_name, source_def))
        return out

    if isinstance(categories, dict):
        for category_name, source_list in categories.items():
            for source_def in source_list:
                out.append((category_name, source_def))
        return out

    return out


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, future=True)
    SessionLocal = sessionmaker(bind=engine)

    sources_path = Path(__file__).resolve().parent.parent / "config" / "sources.json"
    sources_config = load_sources_config(sources_path)

    db = SessionLocal()
    try:
        for category_name, source_def in iter_category_sources(sources_config):
            print(f"\n=== Category: {category_name} ===")

            origin_uri = source_def.get("origin_uri") or source_def.get("url")
            if not origin_uri:
                print(f"  [SKIP] missing origin_uri/url: {source_def!r}")
                continue

            source_name = (
                source_def.get("name")
                or source_def.get("label")
                or origin_uri.rstrip("/").split("/")[-1]
            )
            default_provenance = None
            if isinstance(sources_config.get("categories"), list):
                for c in sources_config["categories"]:
                    if c.get("name") == category_name:
                        default_provenance = c.get("default_provenance")
                        break
            provenance_hint = source_def.get("provenance_hint", default_provenance)

            existing = db.scalars(select(Source).where(Source.origin_uri == origin_uri)).first()
            if existing:
                source = existing
                print(f"  [EXISTS] {source_name}")
            else:
                source = Source(
                    source_kind="git_repo",
                    origin_uri=origin_uri,
                    owner_name=source_def.get("owner"),
                    canonical_source_name=source_name,
                    provenance_hint=provenance_hint,
                )
                db.add(source)
                db.commit()
                print(f"  [NEW] {source_name}")

            try:
                result = collect_source(db, source.source_id)
                rev = (result.revision_ref or "")[:8]
                print(f"    -> {result.capture_status} (rev: {rev})")
            except Exception as e:
                print(f"    -> ERROR: {e}")

    finally:
        db.close()


if __name__ == "__main__":
    main()
