#!/usr/bin/env python3
"""Build dist/catalog/catalog.json from processed directives (file-based)."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def git_sha(repo_root: Path) -> str:
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if proc.returncode == 0 and proc.stdout.strip():
            return proc.stdout.strip()
    except (OSError, subprocess.TimeoutExpired):
        pass
    return "unknown"


def flatten_scores_for_catalog(scores: dict[str, dict[str, float]]) -> dict[str, dict[str, Any]]:
    """Catalog entries: dimension_key -> {score, dimension_group}."""
    out: dict[str, dict[str, Any]] = {}
    for group, dims in scores.items():
        for key, val in dims.items():
            out[key] = {"score": round(float(val), 4), "dimension_group": group, "confidence": 0.7}
    return out


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    root = Path(__file__).resolve().parent.parent
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--input",
        type=Path,
        default=root / "storage" / "processed" / "directives.json",
    )
    p.add_argument(
        "--out-dir",
        type=Path,
        default=root / "dist" / "catalog",
    )
    p.add_argument("--version", default="0.1.0", help="Catalog semver string")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    root = Path(__file__).resolve().parent.parent

    if not args.input.is_file():
        print(f"ERROR: missing {args.input}", file=sys.stderr)
        return 1

    with open(args.input, encoding="utf-8") as f:
        data = json.load(f)

    directives_in = data.get("directives") or []
    build_id = git_sha(root)
    ts = datetime.now(timezone.utc).isoformat()

    catalog_directives: list[dict[str, Any]] = []
    for d in directives_in:
        scores_nested = d.get("scores") or {}
        entry = {
            "directive_id": d.get("directive_id"),
            "preferred_name": d.get("preferred_name"),
            "category": d.get("category"),
            "normalized_summary": d.get("normalized_summary"),
            "status": "active",
            "provenance_hint": d.get("provenance_hint"),
            "origin_uri": d.get("origin_uri"),
            "source_label": d.get("source_label"),
            "overall_score": d.get("overall_score"),
            "scores": flatten_scores_for_catalog(scores_nested),
            "scores_by_group": scores_nested,
            "verdict": {
                "recommendation": d.get("verdict"),
                "trust": "high" if (scores_nested.get("governance") or {}).get("trust", 0) >= 0.65 else "medium",
            },
            "metadata": d.get("metadata"),
        }
        catalog_directives.append(entry)

    catalog = {
        "version": args.version,
        "build_id": build_id,
        "timestamp": ts,
        "instance": "ai-store",
        "directive_count": len(catalog_directives),
        "directives": catalog_directives,
    }

    args.out_dir.mkdir(parents=True, exist_ok=True)
    out_path = args.out_dir / "catalog.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=2)

    print(f"Wrote {len(catalog_directives)} directives to {out_path} (build_id={build_id[:8]}...)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
