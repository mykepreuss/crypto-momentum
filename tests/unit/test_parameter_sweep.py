from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


def _load_parameter_sweep_module():
    repo_root = Path(__file__).resolve().parents[2]
    path = repo_root / "scripts" / "parameter_sweep.py"
    spec = importlib.util.spec_from_file_location("parameter_sweep", path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


parameter_sweep = _load_parameter_sweep_module()


def test_parse_grid_args_default_grid() -> None:
    grid = parameter_sweep._parse_grid_args([])
    assert "min_dv_1m_usd" in grid
    assert "dvz_min" in grid
    assert "entry_score_threshold" in grid
    assert "extension_max" in grid


def test_parse_grid_args_rejects_unknown_key() -> None:
    with pytest.raises(ValueError):
        parameter_sweep._parse_grid_args(["nope=1,2,3"])


def test_parse_grid_args_casts_types() -> None:
    grid = parameter_sweep._parse_grid_args(["stall_minutes=5,10", "dvz_min=-0.5,1.25"])
    assert grid["stall_minutes"] == [5, 10]
    assert grid["dvz_min"] == [-0.5, 1.25]


def test_sampled_param_sets_is_deterministic() -> None:
    keys = ["a", "b"]
    values = [[1, 2, 3], [10, 20]]

    out1 = parameter_sweep._sampled_param_sets(keys=keys, values=values, sample=4, seed=123)
    out2 = parameter_sweep._sampled_param_sets(keys=keys, values=values, sample=4, seed=123)
    assert out1 == out2
