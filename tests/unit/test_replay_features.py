from __future__ import annotations

from decimal import Decimal

from app.candles import Candle
from app.features import compute_features_with_reason
from app.replay import CandlePoint, MINUTE_MS, RollingFeatureState


def _mk_candle(t: int, price: float, vol: float) -> Candle:
    # Keep highs/lows around close so VWAP/extension stays well-defined.
    c = Decimal(str(price))
    v = Decimal(str(vol))
    return Candle(t=t, o=c, h=c, l=c, c=c, v=v)


def _mk_point(t: int, price: float, vol: float) -> CandlePoint:
    return CandlePoint(t=t, o=price, h=price, l=price, c=price, v=vol)


def _mk_point_from_candle(c: Candle) -> CandlePoint:
    return CandlePoint(t=int(c.t), o=float(c.o), h=float(c.h), l=float(c.l), c=float(c.c), v=float(c.v))


def test_replay_features_match_live_compute_for_contiguous_series() -> None:
    # Build 3 hours of simple price action with non-zero volume.
    minutes = 180
    symbol: list[Candle] = []
    baseline: list[Candle] = []

    for i in range(minutes):
        t = i * MINUTE_MS
        # Deterministic but non-constant prices/volumes.
        p_sym = 10.0 + (i * 0.001) + (0.01 if (i % 17 == 0) else 0.0)
        p_btc = 100.0 + (i * 0.002)
        v_sym = 1.0 + (i % 5)
        v_btc = 5.0 + (i % 7)
        symbol.append(_mk_candle(t, p_sym, v_sym))
        baseline.append(_mk_candle(t, p_btc, v_btc))

    sym_state = RollingFeatureState()
    btc_state = RollingFeatureState()

    for i in range(minutes):
        t0 = i * MINUTE_MS
        sym_state.ingest(_mk_point_from_candle(symbol[i]))
        btc_state.ingest(_mk_point_from_candle(baseline[i]))

        # Only compare once enough history exists (trend filter needs >=21 closed 5m buckets).
        if t0 < 110 * MINUTE_MS:
            continue

        fs_live, reason_live = compute_features_with_reason(symbol, baseline, t0=t0)
        fs_replay, reason_replay = sym_state.compute_features_with_reason(baseline=btc_state, t0=t0)

        assert reason_replay == reason_live
        assert fs_replay is not None
        assert fs_live is not None

        assert fs_replay.t0 == fs_live.t0
        assert abs(fs_replay.price - fs_live.price) < 1e-9
        assert abs(fs_replay.rel_r15 - fs_live.rel_r15) < 1e-9
        assert abs(fs_replay.accel - fs_live.accel) < 1e-9
        assert abs(fs_replay.dv_z - fs_live.dv_z) < 1e-6
        assert abs(fs_replay.avg_dv_1m - fs_live.avg_dv_1m) < 1e-6
        assert abs(fs_replay.extension - fs_live.extension) < 1e-6
        assert fs_replay.breakout == fs_live.breakout
        assert fs_replay.trend_ok == fs_live.trend_ok


def test_replay_features_reset_on_gap_matches_live_missing_offsets() -> None:
    # Gap at minute 60 means t0=61m is missing required offsets.
    symbol: list[Candle] = []
    baseline: list[Candle] = []

    sym_state = RollingFeatureState()
    btc_state = RollingFeatureState()

    replay_reason_at_61 = None
    for i in range(0, 70):
        if i == 60:
            continue
        t = i * MINUTE_MS
        symbol.append(_mk_candle(t, 10.0 + i * 0.001, 1.0))
        baseline.append(_mk_candle(t, 100.0 + i * 0.002, 5.0))
        sym_state.ingest(_mk_point_from_candle(symbol[-1]))
        btc_state.ingest(_mk_point_from_candle(baseline[-1]))
        if i == 61:
            _fs, replay_reason_at_61 = sym_state.compute_features_with_reason(baseline=btc_state, t0=t)

    t0 = 61 * MINUTE_MS
    fs_live, reason_live = compute_features_with_reason(symbol, baseline, t0=t0)

    assert fs_live is None
    assert reason_live == "missing_offsets"
    assert replay_reason_at_61 == "missing_offsets"


def test_replay_breakout_window_includes_20th_prior_candle() -> None:
    # Construct a scenario where the high exactly 20 minutes before t0 is the max prior high.
    # Live breakout window includes that candle; replay must match.
    minutes = 140
    symbol: list[Candle] = []
    baseline: list[Candle] = []

    # Baseline: simple monotonic series with non-zero volume.
    for i in range(minutes):
        t = i * MINUTE_MS
        baseline.append(_mk_candle(t, 100.0 + (i * 0.001), 5.0))

    # Symbol: mostly flat, but set a spike in high exactly 20 minutes before t0.
    t0 = (minutes - 1) * MINUTE_MS
    spike_t = t0 - 20 * MINUTE_MS
    for i in range(minutes):
        t = i * MINUTE_MS
        price = 10.0
        high = 10.0
        if t == spike_t:
            high = 100.0
        if t == t0:
            price = 50.0
            high = 50.0
        c = Decimal(str(price))
        h = Decimal(str(high))
        symbol.append(Candle(t=t, o=c, h=h, l=c, c=c, v=Decimal("1")))

    fs_live, reason_live = compute_features_with_reason(symbol, baseline, t0=t0)
    assert reason_live is None
    assert fs_live is not None

    sym_state = RollingFeatureState()
    btc_state = RollingFeatureState()
    for i in range(minutes):
        sym_state.ingest(_mk_point_from_candle(symbol[i]))
        btc_state.ingest(_mk_point_from_candle(baseline[i]))

    fs_replay, reason_replay = sym_state.compute_features_with_reason(baseline=btc_state, t0=t0)
    assert reason_replay is None
    assert fs_replay is not None
    assert fs_replay.breakout == fs_live.breakout
    assert fs_replay.breakout == 0
