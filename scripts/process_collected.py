#!/usr/bin/env python3
"""Process collected GitHub JSON files into scored directives (rule-based, no LLM)."""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from pathlib import Path
from typing import Any

# Heuristic keyword buckets (lowercase matching)
SKILL_HINTS = re.compile(
    r"\b(skill|skills|agent|mcp|plugin|plugins|claude|cursor|codex|gemini|subagent|"
    r"prompt|directive|awesome|rules?|workflow)\b",
    re.I,
)
WORKFLOW_HINTS = re.compile(
    r"\b(workflow|automation|ci|cd|pipeline|orchestrat|playbook|runbook|process)\b",
    re.I,
)
TOOL_HINTS = re.compile(
    r"\b(tool|tools|mcp|api|cli|sdk|server|servers|integration|connector)\b",
    re.I,
)
CODING_README = re.compile(
    r"\b(install|npm|pip|pnpm|yarn|cargo|go install|clone|docker|compose|build|"
    r"typescript|python|rust|```)\b",
    re.I,
)
REVIEW_README = re.compile(
    r"\b(review|pr|pull request|lint|test|coverage|ci|codecov|eslint|ruff)\b",
    re.I,
)
PLANNING_README = re.compile(
    r"\b(plan|roadmap|architecture|design|rfc|todo|backlog|milestone|strategy)\b",
    re.I,
)

TRUST_ORGS = frozenset(
    {
        "anthropics",
        "microsoft",
        "google",
        "google-gemini",
        "google-labs-code",
        "vercel-labs",
        "modelcontextprotocol",
        "openclaw",
        "github",
    }
)

PERMISSIVE_LICENSES = frozenset(
    {
        "MIT",
        "Apache-2.0",
        "BSD-2-Clause",
        "BSD-3-Clause",
        "ISC",
        "Unlicense",
        "0BSD",
        "CC0-1.0",
    }
)
COPYLEFT_LICENSES = frozenset({"GPL-3.0", "GPL-2.0", "AGPL-3.0", "LGPL-3.0", "LGPL-2.1", "MPL-2.0"})


def clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def score_topics_description(description: str, topics: list[str]) -> tuple[float, float, float]:
    text = f"{description} {' '.join(topics)}".lower()
    skill = 0.25
    if SKILL_HINTS.search(text):
        skill += 0.35
    if "awesome" in text or "list" in text:
        skill += 0.15
    if len(topics) >= 5:
        skill += 0.15
    wf = 0.2
    if WORKFLOW_HINTS.search(text):
        wf += 0.45
    if "automation" in text or "workflow" in topics:
        wf += 0.2
    tool = 0.2
    if TOOL_HINTS.search(text):
        tool += 0.45
    if any(t in ("mcp", "cli", "api") for t in topics):
        tool += 0.2
    return clamp01(skill), clamp01(wf), clamp01(tool)


def score_readme_function(readme: str) -> tuple[float, float, float]:
    if not readme.strip():
        return 0.15, 0.1, 0.1
    coding = 0.2
    if CODING_README.search(readme):
        coding += 0.35
    if readme.count("```") >= 2:
        coding += 0.25
    if len(readme) > 800:
        coding += 0.1
    review = 0.15
    if REVIEW_README.search(readme):
        review += 0.45
    plan = 0.15
    if PLANNING_README.search(readme):
        plan += 0.45
    if len(readme) > 1500:
        plan += 0.1
    return clamp01(coding), clamp01(review), clamp01(plan)


def score_installability(license_spdx: str | None, language: str | None) -> float:
    base = 0.35
    if language:
        base += 0.25
    if not license_spdx or license_spdx in ("NOASSERTION", "NONE"):
        return clamp01(base * 0.6)
    if license_spdx in PERMISSIVE_LICENSES:
        base += 0.35
    elif license_spdx in COPYLEFT_LICENSES:
        base += 0.2
    else:
        base += 0.15
    return clamp01(base)


def score_trust(stars: int, owner_login: str | None, owner_type: str | None, provenance: str | None) -> float:
    s = 0.25
    if stars >= 5000:
        s += 0.35
    elif stars >= 1000:
        s += 0.28
    elif stars >= 200:
        s += 0.18
    elif stars >= 50:
        s += 0.1
    ol = (owner_login or "").lower()
    if ol in TRUST_ORGS:
        s += 0.2
    if owner_type == "Organization":
        s += 0.08
    if provenance == "official":
        s += 0.12
    return clamp01(s)


def score_popularity(stars: int, forks: int) -> float:
    # Log-scaled blend
    if stars <= 0 and forks <= 0:
        return 0.12
    ls = math.log1p(stars)
    lf = math.log1p(forks)
    combined = (ls * 0.75 + lf * 0.25) / math.log1p(20000)
    return clamp01(combined)


def score_description_clarity(description: str) -> float:
    d = (description or "").strip()
    n = len(d)
    if n < 20:
        return 0.15
    if n < 40:
        return 0.35
    score = 0.45
    if 60 <= n <= 400:
        score += 0.3
    elif n > 400:
        score += 0.15
    if any(c in d for c in ".:,-"):
        score += 0.1
    if d[0].isupper():
        score += 0.05
    return clamp01(score)


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


def load_collected_files(root: Path) -> list[Path]:
    if not root.is_dir():
        return []
    return sorted(p for p in root.rglob("*.json") if p.is_file())


def directive_from_file(path: Path) -> dict[str, Any] | None:
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    if data.get("error") or not data.get("repo"):
        return None
    repo = data["repo"]
    desc = repo.get("description") or ""
    topics = list(repo.get("topics") or [])
    readme = data.get("readme_excerpt") or ""
    stars = int(repo.get("stargazers_count") or 0)
    forks = int(repo.get("forks_count") or 0)
    lic = repo.get("license_spdx")
    lang = repo.get("language")
    owner_login = repo.get("owner_login")
    owner_type = repo.get("owner_type")
    provenance = data.get("provenance_hint")

    sk, wf, tl = score_topics_description(desc, topics)
    cd, rv, pl = score_readme_function(readme)
    inst = score_installability(lic, lang)
    trust = score_trust(stars, owner_login, owner_type, provenance)
    pop = score_popularity(stars, forks)
    clar = score_description_clarity(desc)

    scores: dict[str, dict[str, float]] = {
        "form": {"skillness": sk, "workflowness": wf, "toolness": tl},
        "function": {"coding": cd, "review": rv, "planning": pl},
        "execution": {"installability": inst},
        "governance": {"trust": trust},
        "adoption": {"popularity": pop},
        "clarity": {"description_clarity": clar},
    }
    avg = overall_average(scores)
    verdict = verdict_from_scores(avg, trust, inst)

    directive_id = f"{data.get('category')}/{repo.get('full_name', path.stem)}"

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
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    files = load_collected_files(args.input)
    if not files:
        print(f"No JSON files under {args.input}", file=sys.stderr)
        return 1

    directives: list[dict[str, Any]] = []
    skipped = 0
    for p in files:
        d = directive_from_file(p)
        if d:
            directives.append(d)
        else:
            skipped += 1

    args.output.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": "0.1.0",
        "directive_count": len(directives),
        "directives": directives,
    }
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Processed {len(directives)} directives (skipped {skipped} files) -> {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
