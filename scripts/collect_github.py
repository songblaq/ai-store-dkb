#!/usr/bin/env python3
"""Collect GitHub repo metadata and README excerpt into storage/collected/ (file-based, no DB)."""

from __future__ import annotations

import argparse
import base64
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

GITHUB_REPO_RE = re.compile(
    r"github\.com[/:](?P<owner>[^/]+)/(?P<repo>[^/]+?)(?:\.git)?/?$",
    re.IGNORECASE,
)


def load_sources_config(path: Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def iter_category_sources(
    sources_config: dict[str, Any],
) -> list[tuple[str, dict[str, Any]]]:
    out: list[tuple[str, dict[str, Any]]] = []
    categories = sources_config.get("categories", {})
    if isinstance(categories, dict):
        for category_name, source_list in categories.items():
            for source_def in source_list:
                out.append((category_name, source_def))
    return out


def parse_github_repo(origin_uri: str) -> tuple[str, str] | None:
    if not origin_uri:
        return None
    parsed = urlparse(origin_uri.strip())
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").strip("/")
    if "github.com" in host and path:
        parts = path.split("/")
        if len(parts) >= 2:
            return parts[0], parts[1].removesuffix(".git")
    m = GITHUB_REPO_RE.search(origin_uri)
    if m:
        return m.group("owner"), m.group("repo").removesuffix(".git")
    return None


def safe_filename(owner: str, repo: str) -> str:
    safe = f"{owner}__{repo}".replace("/", "_")
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", safe)


def run_gh_api(path: str) -> tuple[int, dict[str, Any] | None, str]:
    """GET via gh api. Returns (exit_code, json_body_or_none, stderr_snippet)."""
    cmd = ["gh", "api", path, "--method", "GET"]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
        )
    except FileNotFoundError:
        print(
            "ERROR: gh CLI not found. Install GitHub CLI and run `gh auth login`.",
            file=sys.stderr,
        )
        return 127, None, ""
    except subprocess.TimeoutExpired:
        return 124, None, "timeout"

    err = (proc.stderr or "").strip()
    if proc.returncode != 0:
        return proc.returncode, None, err

    try:
        data = json.loads(proc.stdout or "{}")
    except json.JSONDecodeError:
        return 1, None, err
    return 0, data, err


def fetch_readme_excerpt(owner: str, repo: str, max_chars: int = 500) -> str:
    api_path = f"repos/{owner}/{repo}/readme"
    code, data, _ = run_gh_api(api_path)
    if code != 0 or not data:
        return ""
    b64 = (data.get("content") or "").replace("\n", "")
    if not b64:
        return ""
    try:
        raw = base64.b64decode(b64.encode("ascii"), validate=False).decode(
            "utf-8", errors="replace"
        )
    except (ValueError, OSError):
        return ""
    raw = raw.strip()
    if len(raw) <= max_chars:
        return raw
    return raw[:max_chars]


def fetch_repo_metadata(
    owner: str, repo: str
) -> tuple[dict[str, Any] | None, str | None]:
    api_path = f"repos/{owner}/{repo}"
    code, data, stderr = run_gh_api(api_path)
    if code != 0:
        hint = stderr[:200] if stderr else ""
        err = f"gh api failed (exit {code}) for {api_path}" + (
            f": {hint}" if hint else ""
        )
        return None, err
    return data, None


def sleep_for_rate_limit(last_response_hint: str | None) -> None:
    if last_response_hint and "rate limit" in last_response_hint.lower():
        time.sleep(65)
    else:
        time.sleep(0.35)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    root = Path(__file__).resolve().parent.parent
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--limit",
        type=int,
        default=0,
        metavar="N",
        help="Max sources per category (0 = all).",
    )
    p.add_argument(
        "--sources",
        type=Path,
        default=root / "config" / "sources.json",
        help="Path to sources.json",
    )
    p.add_argument(
        "--out",
        type=Path,
        default=root / "storage" / "collected",
        help="Output directory",
    )
    p.add_argument(
        "--delay",
        type=float,
        default=0.35,
        help="Seconds to sleep between API calls (default 0.35).",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    sources_path = args.sources
    if not sources_path.is_file():
        print(f"ERROR: sources file not found: {sources_path}", file=sys.stderr)
        return 1

    sources_config = load_sources_config(sources_path)
    pairs = iter_category_sources(sources_config)

    by_cat: dict[str, list[dict[str, Any]]] = {}
    for cat, src in pairs:
        by_cat.setdefault(cat, []).append(src)

    out_root = args.out
    out_root.mkdir(parents=True, exist_ok=True)

    total_ok = 0
    total_err = 0
    cat_list = sorted(by_cat.items(), key=lambda x: x[0])

    for cat_idx, (category_name, source_list) in enumerate(cat_list, start=1):
        limited = (
            source_list[: args.limit] if args.limit and args.limit > 0 else source_list
        )
        n = len(limited)
        print(
            f"\n=== Category [{cat_idx}/{len(cat_list)}]: {category_name} ({n} sources) ==="
        )
        cat_dir = out_root / category_name
        cat_dir.mkdir(parents=True, exist_ok=True)

        for src_idx, source_def in enumerate(limited, start=1):
            origin_uri = source_def.get("origin_uri") or source_def.get("url") or ""
            label = source_def.get("label") or origin_uri
            print(f"  [{src_idx}/{n}] {label}")

            parsed = parse_github_repo(origin_uri)
            if not parsed:
                print("    [SKIP] not a github.com repo URL")
                total_err += 1
                err_payload = {
                    "category": category_name,
                    "source_label": label,
                    "provenance_hint": source_def.get("provenance_hint"),
                    "origin_uri": origin_uri,
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                    "error": "unparseable_github_url",
                    "repo": None,
                    "readme_excerpt": "",
                }
                fn = safe_filename("invalid", str(src_idx))
                with open(cat_dir / f"{fn}.json", "w", encoding="utf-8") as f:
                    json.dump(err_payload, f, indent=2)
                time.sleep(args.delay)
                continue

            owner, repo = parsed
            time.sleep(args.delay)
            meta, err = fetch_repo_metadata(owner, repo)
            if err:
                print(f"    [ERROR] {err}")
                # Retry once after pause (rate limit)
                time.sleep(2.0)
                meta, err = fetch_repo_metadata(owner, repo)
            if not meta:
                total_err += 1
                payload = {
                    "category": category_name,
                    "source_label": label,
                    "provenance_hint": source_def.get("provenance_hint"),
                    "origin_uri": origin_uri,
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                    "error": err or "fetch_failed",
                    "repo": None,
                    "readme_excerpt": "",
                }
                out_file = cat_dir / f"{safe_filename(owner, repo)}.json"
                with open(out_file, "w", encoding="utf-8") as f:
                    json.dump(payload, f, indent=2)
                sleep_for_rate_limit(err)
                continue

            time.sleep(args.delay)
            excerpt = fetch_readme_excerpt(owner, repo)

            license_obj = meta.get("license")
            license_spdx = None
            if isinstance(license_obj, dict):
                license_spdx = license_obj.get("spdx_id") or license_obj.get("key")

            topics = meta.get("topics") or []
            owner_obj = meta.get("owner") or {}

            slim_repo = {
                "full_name": meta.get("full_name"),
                "name": meta.get("name"),
                "description": meta.get("description") or "",
                "stargazers_count": meta.get("stargazers_count", 0),
                "forks_count": meta.get("forks_count", 0),
                "language": meta.get("language"),
                "license_spdx": license_spdx,
                "topics": list(topics),
                "updated_at": meta.get("updated_at"),
                "owner_login": owner_obj.get("login"),
                "owner_type": owner_obj.get("type"),
                "html_url": meta.get("html_url"),
            }

            payload = {
                "category": category_name,
                "source_label": label,
                "provenance_hint": source_def.get("provenance_hint"),
                "origin_uri": origin_uri,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "error": None,
                "repo": slim_repo,
                "readme_excerpt": excerpt,
            }
            out_file = cat_dir / f"{safe_filename(owner, repo)}.json"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
            print(f"    -> saved {out_file.name} (★ {slim_repo['stargazers_count']})")
            total_ok += 1

    print("\n=== Collection summary ===")
    print(f"  ok: {total_ok}")
    print(f"  errors/skips: {total_err}")
    print(f"  output: {out_root}")
    return 0 if total_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
