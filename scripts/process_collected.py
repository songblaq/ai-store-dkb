#!/usr/bin/env python3
"""Process collected GitHub JSON files into scored directives (rule-based, no LLM)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from dkb_runtime.services.scoring import _clamp01, _score_dimension

_ROOT = Path(__file__).resolve().parent.parent
_DIMENSION_MODEL_PATH = _ROOT / "config" / "dimension_model_v0_1.json"


def load_dimension_groups(config_path: Path) -> list[tuple[str, list[str]]]:
    data = json.loads(config_path.read_text(encoding="utf-8"))
    return [(g["name"], list(g["dimensions"])) for g in data["groups"]]


def overall_average(scores: dict[str, dict[str, float]]) -> float:
    flat: list[float] = []
    for group in scores.values():
        flat.extend(group.values())
    if not flat:
        return 0.0
    return sum(flat) / len(flat)


def verdict_from_scores(avg: float, trust: float, installability: float) -> str:
    if avg >= 0.62 and trust >= 0.45 and installability >= 0.4:
        return "recommended"
    if avg < 0.38 or trust < 0.28 or installability < 0.25:
        return "caution"
    return "neutral"


def build_scoring_context(
    data: dict[str, Any], repo: dict[str, Any]
) -> tuple[str, str, str]:
    desc = repo.get("description") or ""
    topics = list(repo.get("topics") or [])
    readme = data.get("readme_excerpt") or ""
    full = repo.get("full_name") or ""
    name = repo.get("name") or ""
    stars = int(repo.get("stargazers_count") or 0)
    forks = int(repo.get("forks_count") or 0)
    lic = repo.get("license_spdx") or ""

    parts = [full, name, desc, " ".join(topics), readme]
    parts.append(f"github repository stargazers_count={stars} forks_count={forks}")
    if lic and str(lic).upper() not in ("NOASSERTION", "NONE", ""):
        parts.append(f"license {lic}")

    content = "\n".join(p for p in parts if p)
    path_blob = f"{full.lower()} {' '.join(topics).lower()}"
    type_blob = (data.get("category") or "").lower()
    return content, path_blob, type_blob


def score_directive_from_context(
    content: str,
    path_blob: str,
    type_blob: str,
    dimension_config: Path,
) -> dict[str, dict[str, float]]:
    groups = load_dimension_groups(dimension_config)
    scores: dict[str, dict[str, float]] = {}
    for group_name, dims in groups:
        scores[group_name] = {}
        for dim_key in dims:
            s, _conf, _ex = _score_dimension(
                group_name, dim_key, content, path_blob, type_blob
            )
            scores[group_name][dim_key] = _clamp01(float(s))
    return scores


def load_collected_files(root: Path) -> list[Path]:
    if not root.is_dir():
        return []
    return sorted(p for p in root.rglob("*.json") if p.is_file())


def directive_from_file(path: Path, dimension_config: Path) -> dict[str, Any] | None:
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    if data.get("error") or not data.get("repo"):
        return None
    repo = data["repo"]
    desc = repo.get("description") or ""
    readme = data.get("readme_excerpt") or ""

    if not dimension_config.is_file():
        print(f"ERROR: dimension model missing: {dimension_config}", file=sys.stderr)
        return None

    content, path_blob, type_blob = build_scoring_context(data, repo)
    scores = score_directive_from_context(
        content, path_blob, type_blob, dimension_config
    )

    gov = scores.get("governance") or {}
    trust = float(gov.get("trustworthiness", 0.0))
    inst = float(gov.get("install_verifiability", 0.0))
    avg = overall_average(scores)
    verdict = verdict_from_scores(avg, trust, inst)

    directive_id = f"{data.get('category')}/{repo.get('full_name', path.stem)}"

    provenance = data.get("provenance_hint")

    return {
        "directive_id": directive_id,
        "preferred_name": repo.get("full_name") or path.stem,
        "category": data.get("category"),
        "source_label": data.get("source_label"),
        "provenance_hint": provenance,
        "origin_uri": data.get("origin_uri"),
        "collected_path": str(path),
        "normalized_summary": desc[:500] if desc else None,
        "metadata": {
            "repo": repo,
            "readme_excerpt": readme,
            "fetched_at": data.get("fetched_at"),
        },
        "scores": scores,
        "overall_score": round(avg, 4),
        "verdict": verdict,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    root = Path(__file__).resolve().parent.parent
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--input",
        type=Path,
        default=root / "storage" / "collected",
        help="Directory of collected JSON files",
    )
    p.add_argument(
        "--output",
        type=Path,
        default=root / "storage" / "processed" / "directives.json",
        help="Output directives JSON path",
    )
    p.add_argument(
        "--dimension-model",
        type=Path,
        default=root / "config" / "dimension_model_v0_1.json",
        help="Path to dimension_model_v0_1.json (34 DG dimensions)",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    dim_cfg = args.dimension_model
    files = load_collected_files(args.input)
    if not files:
        print(f"No JSON files under {args.input}", file=sys.stderr)
        return 1

    directives: list[dict[str, Any]] = []
    skipped = 0
    for p in files:
        d = directive_from_file(p, dim_cfg)
        if d:
            directives.append(d)
        else:
            skipped += 1

    args.output.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": "0.2.0",
        "directive_count": len(directives),
        "dimension_model_path": str(dim_cfg),
        "directives": directives,
    }
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(
        f"Processed {len(directives)} directives (skipped {skipped} files) -> {args.output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
