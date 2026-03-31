"""Tests for run_pipeline (full run needs Postgres + extractor/canonicalizer/scoring/verdict)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


def _load_run_pipeline():
    path = ROOT / "scripts" / "run_pipeline.py"
    name = "run_pipeline_test_mod"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def test_run_pipeline_module_loads():
    mod = _load_run_pipeline()
    assert callable(mod.main)


@pytest.mark.skip(reason="Requires live DB and full dkb_runtime pipeline services")
def test_run_pipeline_end_to_end():
    raise AssertionError("unreachable")
