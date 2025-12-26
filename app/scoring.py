from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def percentile_ranks(values_by_symbol: dict[str, float]) -> dict[str, float]:
    items = list(values_by_symbol.items())
    if not items:
        return {}
    if len(items) == 1:
        sym, _v = items[0]
        return {sym: 1.0}

    items.sort(key=lambda x: x[1])
    n = len(items)
    ranks: dict[str, float] = {}
    i = 0
    while i < n:
        j = i + 1
        while j < n and items[j][1] == items[i][1]:
            j += 1
        avg_rank = (i + (j - 1)) / 2.0
        pct = avg_rank / (n - 1)
        for k in range(i, j):
            ranks[items[k][0]] = pct
        i = j
    return ranks


@dataclass(frozen=True)
class ScoreComponents:
    rank_rel_r15: float
    rank_accel: float
    dv_term: float
    breakout: int


def compute_score(components: ScoreComponents) -> float:
    # Spec formula (simple + explainable).
    return (
        0.45 * components.rank_rel_r15
        + 0.35 * components.rank_accel
        + 0.20 * components.dv_term
        + 0.10 * float(components.breakout)
    )


def dv_term_from_dvz(dv_z: float) -> float:
    # Clamp dv_z to [-3, +6] then scale to roughly [-0.5, +1.0] range.
    return clamp(dv_z, -3.0, 6.0) / 6.0

