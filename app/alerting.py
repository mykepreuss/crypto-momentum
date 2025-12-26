from __future__ import annotations

from typing import Any, Optional


ALERT_TYPE_MOMENTUM_UP = "MOMENTUM_UP"
ALERT_TYPE_MOMENTUM_SLOWING = "MOMENTUM_SLOWING"


def format_momentum_up(symbol: str, features: dict[str, Any]) -> str:
    score = float(features.get("score", 0.0))
    rel15 = float(features.get("rel_r15", 0.0))
    dv_z = float(features.get("dv_z", 0.0))
    ext = float(features.get("extension", 0.0))
    spread = features.get("spread")
    spread_s = "n/a" if spread is None else f"{float(spread):.2%}"
    return (
        f"MOMENTUM UP: {symbol} | score {score:.2f} | rel15 {rel15:.2%} | "
        f"dv_z {dv_z:.1f} | ext {ext:.1%} | spread {spread_s}"
    )


def format_momentum_slowing(
    symbol: str,
    reason: str,
    features: dict[str, Any],
    peak_price: Optional[float],
) -> str:
    score = float(features.get("score", 0.0))
    dv_z = float(features.get("dv_z", 0.0))
    peak_s = "n/a" if peak_price is None else f"{float(peak_price):.6g}"
    return f"MOMENTUM SLOWING: {symbol} | reason {reason} | score {score:.2f} | dv_z {dv_z:.1f} | last_peak {peak_s}"

