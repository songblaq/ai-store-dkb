#!/usr/bin/env python3
"""Collect all configured sources using dkb-runtime services."""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
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


def _default_provenance_for_category(
    sources_config: dict[str, Any], category_name: str
) -> str | None:
    if not isinstance(sources_config.get("categories"), list):
        return None
    for c in sources_config["categories"]:
        if c.get("name") == category_name:
            return c.get("default_provenance")
    return None


def _group_by_category(
    pairs: list[tuple[str, dict[str, Any]]],
) -> list[tuple[str, list[dict[str, Any]]]]:
    by_cat: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for category_name, source_def in pairs:
        by_cat[category_name].append(source_def)
    return sorted(by_cat.items(), key=lambda x: x[0])


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--category",
        action="append",
        dest="categories",
        metavar="NAME",
        help="Only process this category (repeat for multiple). Default: all categories.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned actions without creating sources or running collection.",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    settings = get_settings()
    engine = create_engine(settings.database_url, future=True)
    SessionLocal = sessionmaker(bind=engine)

    sources_path = Path(__file__).resolve().parent.parent / "config" / "sources.json"
    sources_config = load_sources_config(sources_path)

    pairs = iter_category_sources(sources_config)
    if args.categories:
        wanted = set(args.categories)
        pairs = [(c, s) for c, s in pairs if c in wanted]
        unknown = wanted - {c for c, _ in pairs}
        if unknown:
            print(
                f"Warning: no sources found for categories: {', '.join(sorted(unknown))}",
                file=sys.stderr,
            )

    grouped = _group_by_category(pairs)
    n_categories = len(grouped)

    n_skipped = 0
    n_failed = 0
    n_collected = 0
    n_dry_new = 0
    n_dry_existing = 0

    db = SessionLocal()
    try:
        for cat_idx, (category_name, source_list) in enumerate(grouped, start=1):
            print(f"\n=== Category [{cat_idx}/{n_categories}]: {category_name} ===")
            n_sources = len(source_list)

            for src_idx, source_def in enumerate(source_list, start=1):
                origin_uri = source_def.get("origin_uri") or source_def.get("url")
                source_name = (
                    source_def.get("name")
                    or source_def.get("label")
                    or (origin_uri or "").rstrip("/").split("/")[-1]
                    or "(unnamed)"
                )

                print(f"  [{src_idx}/{n_sources}] {source_name}")

                if not origin_uri:
                    print("    [SKIP] missing origin_uri/url")
                    n_skipped += 1
                    continue

                default_provenance = _default_provenance_for_category(
                    sources_config, category_name
                )
                provenance_hint = source_def.get("provenance_hint", default_provenance)

                existing = db.scalars(
                    select(Source).where(Source.origin_uri == origin_uri)
                ).first()

                if args.dry_run:
                    if existing:
                        print(
                            f"    [DRY-RUN] would collect existing source (id={existing.source_id})"
                        )
                        n_dry_existing += 1
                    else:
                        print(
                            "    [DRY-RUN] would create Source and run collect_source "
                            f"(uri={origin_uri!r}, provenance={provenance_hint!r})"
                        )
                        n_dry_new += 1
                    continue

                if existing:
                    source = existing
                    print("    [EXISTS] reusing source row")
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
                    print("    [NEW] inserted Source row")

                try:
                    result = collect_source(db, source.source_id)
                    rev = (result.revision_ref or "")[:8]
                    print(f"    -> {result.capture_status} (rev: {rev})")
                    if result.capture_status == "captured":
                        n_collected += 1
                    else:
                        n_failed += 1
                except Exception as e:
                    print(f"    -> ERROR: {e}")
                    n_failed += 1

        print("\n=== Collection summary ===")
        if args.dry_run:
            print("  dry_run: yes")
            print(f"  categories processed: {n_categories}")
            print(f"  skipped (missing uri): {n_skipped}")
            print(f"  would reuse existing: {n_dry_existing}")
            print(f"  would create new: {n_dry_new}")
        else:
            print(f"  collected (capture ok): {n_collected}")
            print(f"  failed: {n_failed}")
            print(f"  skipped (missing uri): {n_skipped}")

    finally:
        db.close()


if __name__ == "__main__":
    main()
