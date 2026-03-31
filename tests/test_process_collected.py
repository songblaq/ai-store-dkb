"""Tests for file-based process_collected (34 DG dimensions)."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture
def dimension_model_path() -> Path:
    p = ROOT / "config" / "dimension_model_v0_1.json"
    assert p.is_file()
    return p


def test_dimension_model_has_34_leaves(dimension_model_path: Path) -> None:
    data = json.loads(dimension_model_path.read_text(encoding="utf-8"))
    n = sum(len(g["dimensions"]) for g in data["groups"])
    assert n == 34


def test_directive_from_file_scores_34_dimensions(
    dimension_model_path: Path,
    tmp_path: Path,
) -> None:
    path = ROOT / "scripts" / "process_collected.py"
    name = "process_collected_test_mod"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)

    sample = {
        "category": "mcp",
        "source_label": "test",
        "provenance_hint": "official",
        "origin_uri": "https://github.com/modelcontextprotocol/servers",
        "fetched_at": "2026-01-01T00:00:00Z",
        "error": None,
        "repo": {
            "full_name": "modelcontextprotocol/servers",
            "name": "servers",
            "description": "Model Context Protocol servers",
            "stargazers_count": 5000,
            "forks_count": 500,
            "language": "TypeScript",
            "license_spdx": "MIT",
            "topics": ["mcp", "agents"],
            "updated_at": "2026-01-01T00:00:00Z",
            "owner_login": "modelcontextprotocol",
            "owner_type": "Organization",
            "html_url": "https://github.com/modelcontextprotocol/servers",
        },
        "readme_excerpt": "```bash\nnpm install\n```\nRun an MCP server.",
    }
    fp = tmp_path / "mcp__servers.json"
    fp.write_text(json.dumps(sample), encoding="utf-8")

    d = mod.directive_from_file(fp, dimension_model_path)
    assert d is not None
    scores = d["scores"]
    flat = [v for g in scores.values() for v in g.values()]
    assert len(flat) == 34
    for v in flat:
        assert 0.0 <= float(v) <= 1.0
    assert d["verdict"] in ("recommended", "neutral", "caution")
