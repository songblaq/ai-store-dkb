#!/usr/bin/env python3
"""E2E validation: collect three target repos, run the file-based pipeline, check 34 DG dimensions."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
_SCRIPTS = _ROOT / "scripts"
_DEFAULT_SOURCES = _ROOT / "config" / "e2e_validation_sources.json"
_DEFAULT_COLLECTED = _ROOT / "data" / "validation" / "e2e_collected"
_DEFAULT_PROCESSED = _ROOT / "data" / "validation" / "processed" / "directives.json"
_DEFAULT_REPORT_DIR = _ROOT / "data" / "validation"
_DIMENSION_MODEL = _ROOT / "config" / "dimension_model_v0_1.json"

REQUIRED_DIRECTIVE_FIELDS = (
    "directive_id",
    "preferred_name",
    "category",
    "source_label",
    "provenance_hint",
    "origin_uri",
    "scores",
    "overall_score",
    "verdict",
    "metadata",
)


@dataclass
class RepoResult:
    full_name: str
    issue: int
    ok: bool
    errors: list[str] = field(default_factory=list)
    directive: dict[str, Any] | None = None


def load_dimension_expectations(path: Path) -> tuple[int, dict[str, list[str]]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    groups = data["groups"]
    by_group: dict[str, list[str]] = {g["name"]: list(g["dimensions"]) for g in groups}
    n = sum(len(v) for v in by_group.values())
    return n, by_group


def run_subprocess(cmd: list[str], cwd: Path) -> tuple[int, str]:
    proc = subprocess.run(
        cmd, cwd=str(cwd), capture_output=True, text=True, timeout=600
    )
    out = (proc.stdout or "") + (proc.stderr or "")
    return proc.returncode, out


def collect_sources(
    sources_path: Path,
    out_dir: Path,
    delay: float,
) -> tuple[int, str]:
    cmd = [
        sys.executable,
        str(_SCRIPTS / "collect_github.py"),
        "--sources",
        str(sources_path),
        "--out",
        str(out_dir),
        "--delay",
        str(delay),
    ]
    return run_subprocess(cmd, _ROOT)


def process_collected(
    collected_dir: Path,
    processed_path: Path,
    dimension_model: Path,
) -> tuple[int, str]:
    cmd = [
        sys.executable,
        str(_SCRIPTS / "process_collected.py"),
        "--input",
        str(collected_dir),
        "--output",
        str(processed_path),
        "--dimension-model",
        str(dimension_model),
    ]
    return run_subprocess(cmd, _ROOT)


def validate_scores(
    scores: dict[str, dict[str, Any]],
    expected: dict[str, list[str]],
) -> list[str]:
    errs: list[str] = []
    for group, keys in expected.items():
        if group not in scores:
            errs.append(f"missing score group {group!r}")
            continue
        gmap = scores[group]
        for k in keys:
            if k not in gmap:
                errs.append(f"missing dimension {group}.{k}")
                continue
            v = gmap[k]
            if not isinstance(v, (int, float)):
                errs.append(f"non-numeric {group}.{k}={v!r}")
                continue
            if not (0.0 <= float(v) <= 1.0):
                errs.append(f"out of range [0,1] {group}.{k}={v}")
    for group in scores:
        if group not in expected:
            errs.append(f"unexpected score group {group!r}")
    return errs


def validate_directive(
    d: dict[str, Any],
    expected: dict[str, list[str]],
) -> list[str]:
    errs: list[str] = []
    for f in REQUIRED_DIRECTIVE_FIELDS:
        if f not in d or d[f] is None:
            errs.append(f"missing required field {f!r}")
    scores = d.get("scores")
    if not isinstance(scores, dict):
        errs.append("scores must be a dict")
        return errs
    errs.extend(validate_scores(scores, expected))
    os_ = d.get("overall_score")
    if not isinstance(os_, (int, float)) or not (0.0 <= float(os_) <= 1.0):
        errs.append(f"overall_score invalid: {os_!r}")
    if d.get("verdict") not in ("recommended", "neutral", "caution"):
        errs.append(f"verdict invalid: {d.get('verdict')!r}")
    return errs


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--sources",
        type=Path,
        default=_DEFAULT_SOURCES,
        help="Fragment sources.json for collect_github.py",
    )
    p.add_argument(
        "--collected-dir",
        type=Path,
        default=_DEFAULT_COLLECTED,
        help="Directory for collected JSON (per category)",
    )
    p.add_argument(
        "--processed",
        type=Path,
        default=_DEFAULT_PROCESSED,
        help="Output path for process_collected.py",
    )
    p.add_argument(
        "--report-dir",
        type=Path,
        default=_DEFAULT_REPORT_DIR,
        help="Directory for validation_report.json / .md",
    )
    p.add_argument(
        "--dimension-model",
        type=Path,
        default=_DIMENSION_MODEL,
    )
    p.add_argument(
        "--skip-collect",
        action="store_true",
        help="Reuse existing --collected-dir (no gh api)",
    )
    p.add_argument(
        "--delay",
        type=float,
        default=0.35,
        help="Delay between GitHub API calls when collecting",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    n_expected, expected_groups = load_dimension_expectations(args.dimension_model)
    if n_expected != 34:
        print(
            f"ERROR: expected 34 dimensions, found {n_expected} in {args.dimension_model}",
            file=sys.stderr,
        )
        return 1

    args.report_dir.mkdir(parents=True, exist_ok=True)
    args.collected_dir.mkdir(parents=True, exist_ok=True)
    args.processed.parent.mkdir(parents=True, exist_ok=True)

    notes = [
        "Issue #10: target anthropics/skills (official Anthropic public skills). "
        "The path anthropic/claude-code-skills is not a public GitHub repository as of validation.",
        "Issue #11: hesreallyhim/awesome-claude-code",
        "Issue #12: modelcontextprotocol/servers",
    ]

    if not args.skip_collect:
        if not args.sources.is_file():
            print(f"ERROR: sources file not found: {args.sources}", file=sys.stderr)
            return 1
        code, out = collect_sources(args.sources, args.collected_dir, args.delay)
        if code != 0:
            print(out, file=sys.stderr)
            print(f"ERROR: collect_github.py exited {code}", file=sys.stderr)
            return code

    code, out = process_collected(
        args.collected_dir, args.processed, args.dimension_model
    )
    if code != 0:
        print(out, file=sys.stderr)
        print(f"ERROR: process_collected.py exited {code}", file=sys.stderr)
        return code

    with open(args.processed, encoding="utf-8") as f:
        bundle = json.load(f)

    directives = bundle.get("directives") or []
    issue_by_name = {
        "anthropics/skills": 10,
        "hesreallyhim/awesome-claude-code": 11,
        "modelcontextprotocol/servers": 12,
    }

    repo_results: list[RepoResult] = []
    all_ok = True

    for d in directives:
        name = d.get("preferred_name") or ""
        iss = issue_by_name.get(name, 0)
        errs = validate_directive(d, expected_groups)
        ok = not errs
        all_ok = all_ok and ok
        repo_results.append(
            RepoResult(full_name=name, issue=iss, ok=ok, errors=errs, directive=d)
        )

    missing = set(issue_by_name) - {r.full_name for r in repo_results}
    for fn in sorted(missing):
        all_ok = False
        repo_results.append(
            RepoResult(
                full_name=fn,
                issue=issue_by_name[fn],
                ok=False,
                errors=[f"no directive produced for {fn} (collect or process failed)"],
            )
        )

    ts = datetime.now(timezone.utc).isoformat()
    report = {
        "timestamp": ts,
        "dimension_count_expected": n_expected,
        "overall_ok": all_ok,
        "processed_path": str(args.processed),
        "collected_dir": str(args.collected_dir),
        "notes": notes,
        "repos": [
            {
                "full_name": r.full_name,
                "github_issue": r.issue,
                "ok": r.ok,
                "errors": r.errors,
                "overall_score": (r.directive or {}).get("overall_score"),
                "verdict": (r.directive or {}).get("verdict"),
            }
            for r in sorted(repo_results, key=lambda x: x.issue or 99)
        ],
    }

    json_path = args.report_dir / "validation_report.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    md_lines = [
        "# E2E validation report",
        "",
        f"- **UTC**: {ts}",
        f"- **Overall**: {'PASS' if all_ok else 'FAIL'}",
        f"- **Dimensions**: {n_expected} (DG v0.1 model)",
        "",
        "## Repositories",
        "",
    ]
    for row in report["repos"]:
        status = "PASS" if row["ok"] else "FAIL"
        md_lines.append(
            f"- **{row['full_name']}** (issue #{row['github_issue']}): {status}"
        )
        if row.get("overall_score") is not None:
            md_lines.append(
                f"  - overall_score: {row['overall_score']}, verdict: {row.get('verdict')}"
            )
        for e in row.get("errors") or []:
            md_lines.append(f"  - ERROR: {e}")
    md_lines.extend(["", "## Notes", ""])
    for n in notes:
        md_lines.append(f"- {n}")

    md_path = args.report_dir / "validation_report.md"
    md_path.write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    print(f"Wrote {json_path} and {md_path}")
    if not all_ok:
        print("VALIDATION FAILED", file=sys.stderr)
        for r in repo_results:
            if not r.ok:
                print(f"  {r.full_name}: {r.errors}", file=sys.stderr)
        return 1

    print("VALIDATION OK (3 repos, 34 dimensions each)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
