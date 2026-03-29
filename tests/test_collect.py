"""Tests for collection pipeline.

Full collection requires PostgreSQL and implemented dkb_runtime.services.collector.
These tests cover config parsing and script structure.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


def _load_collect_module():
    path = ROOT / "scripts" / "collect_all.py"
    spec = importlib.util.spec_from_file_location("collect_all", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_sources_json_loads_and_has_categories():
    sources_path = ROOT / "config" / "sources.json"
    assert sources_path.is_file()
    mod = _load_collect_module()
    cfg = mod.load_sources_config(sources_path)
    assert "categories" in cfg
    assert cfg["categories"]


def test_iter_category_sources_flattens_repo_shape():
    mod = _load_collect_module()
    cfg = mod.load_sources_config(ROOT / "config" / "sources.json")
    pairs = mod.iter_category_sources(cfg)
    assert pairs
    for category_name, source_def in pairs:
        assert isinstance(category_name, str)
        assert "origin_uri" in source_def or "url" in source_def


def test_iter_category_sources_supports_wp3_list_shape():
    mod = _load_collect_module()
    cfg = {
        "categories": [
            {
                "name": "demo",
                "sources": [{"url": "https://example.com/repo", "name": "ex"}],
            }
        ]
    }
    pairs = mod.iter_category_sources(cfg)
    assert pairs == [("demo", {"url": "https://example.com/repo", "name": "ex"})]


@pytest.mark.skip(reason="Requires live DB and collect_source implementation")
def test_collect_all_runs_end_to_end():
    raise AssertionError("unreachable")
