from __future__ import annotations

from decimal import Decimal

import pytest

from app.universe import ParsedInstrument, ParsedTicker, select_universe


def test_select_universe_includes_btc_baseline_even_if_not_top_n() -> None:
    instruments = [
        ParsedInstrument(
            symbol="BTC_USDT",
            inst_type=None,
            base_ccy="BTC",
            quote_ccy="USDT",
            tradable=True,
            display_name=None,
        ),
        ParsedInstrument(
            symbol="ETH_USDT",
            inst_type=None,
            base_ccy="ETH",
            quote_ccy="USDT",
            tradable=True,
            display_name=None,
        ),
        ParsedInstrument(
            symbol="SOL_USDT",
            inst_type=None,
            base_ccy="SOL",
            quote_ccy="USDT",
            tradable=True,
            display_name=None,
        ),
    ]
    tickers = {
        "BTC_USDT": ParsedTicker(
            symbol="BTC_USDT", last_price=Decimal("1"), vol_24h_base=Decimal("1")
        ),
        "ETH_USDT": ParsedTicker(
            symbol="ETH_USDT", last_price=Decimal("10"), vol_24h_base=Decimal("10")
        ),
        "SOL_USDT": ParsedTicker(
            symbol="SOL_USDT", last_price=Decimal("9"), vol_24h_base=Decimal("10")
        ),
    }

    selection = select_universe(
        instruments=instruments,
        tickers=tickers,
        quote_ccy="USDT",
        max_size=2,
        updated_ts=123,
    )

    assert selection.baseline_symbol == "BTC_USDT"
    assert [m.symbol for m in selection.members] == ["ETH_USDT", "SOL_USDT", "BTC_USDT"]
    assert [m.liquidity_rank for m in selection.members] == [1, 2, 3]
    assert sum(1 for m in selection.members if m.is_baseline) == 1


def test_select_universe_filters_by_quote_ccy_and_spot_symbol() -> None:
    instruments = [
        ParsedInstrument(
            symbol="BTC_USDT-PERP",
            inst_type=None,
            base_ccy="BTC",
            quote_ccy="USDT",
            tradable=True,
            display_name=None,
        ),
        ParsedInstrument(
            symbol="ETH_USDC",
            inst_type=None,
            base_ccy="ETH",
            quote_ccy="USDC",
            tradable=True,
            display_name=None,
        ),
        ParsedInstrument(
            symbol="SOL_USDT",
            inst_type=None,
            base_ccy="SOL",
            quote_ccy="USDT",
            tradable=True,
            display_name=None,
        ),
    ]
    tickers = {
        "BTC_USDT-PERP": ParsedTicker(
            symbol="BTC_USDT-PERP", last_price=Decimal("100"), vol_24h_base=Decimal("100")
        ),
        "ETH_USDC": ParsedTicker(
            symbol="ETH_USDC", last_price=Decimal("100"), vol_24h_base=Decimal("100")
        ),
        "SOL_USDT": ParsedTicker(
            symbol="SOL_USDT", last_price=Decimal("1"), vol_24h_base=Decimal("1")
        ),
    }

    selection = select_universe(
        instruments=instruments,
        tickers=tickers,
        quote_ccy="USDT",
        max_size=200,
        updated_ts=1,
    )
    assert [m.symbol for m in selection.members] == ["SOL_USDT"]


def test_select_universe_excludes_pegged_bases_and_symbols() -> None:
    instruments = [
        ParsedInstrument(
            symbol="BTC_USDT",
            inst_type=None,
            base_ccy="BTC",
            quote_ccy="USDT",
            tradable=True,
            display_name=None,
        ),
        ParsedInstrument(
            symbol="EUR_USDT",
            inst_type=None,
            base_ccy="EUR",
            quote_ccy="USDT",
            tradable=True,
            display_name=None,
        ),
        ParsedInstrument(
            symbol="USDC_USDT",
            inst_type=None,
            base_ccy="USDC",
            quote_ccy="USDT",
            tradable=True,
            display_name=None,
        ),
        ParsedInstrument(
            symbol="SOL_USDT",
            inst_type=None,
            base_ccy="SOL",
            quote_ccy="USDT",
            tradable=True,
            display_name=None,
        ),
    ]
    tickers = {
        "BTC_USDT": ParsedTicker(symbol="BTC_USDT", last_price=Decimal("1"), vol_24h_base=Decimal("100")),
        "EUR_USDT": ParsedTicker(symbol="EUR_USDT", last_price=Decimal("1"), vol_24h_base=Decimal("999")),
        "USDC_USDT": ParsedTicker(symbol="USDC_USDT", last_price=Decimal("1"), vol_24h_base=Decimal("999")),
        "SOL_USDT": ParsedTicker(symbol="SOL_USDT", last_price=Decimal("1"), vol_24h_base=Decimal("10")),
    }

    selection = select_universe(
        instruments=instruments,
        tickers=tickers,
        quote_ccy="USDT",
        max_size=200,
        updated_ts=123,
        exclude_base_ccy={"EUR", "USDC"},
        exclude_symbols={"USDC_USDT"},
    )

    symbols = [m.symbol for m in selection.members]
    assert selection.baseline_symbol == "BTC_USDT"
    assert "EUR_USDT" not in symbols
    assert "USDC_USDT" not in symbols
    assert "SOL_USDT" in symbols


def test_select_universe_raises_when_no_candidates() -> None:
    with pytest.raises(ValueError):
        select_universe(
            instruments=[],
            tickers={},
            quote_ccy="USDT",
            max_size=200,
            updated_ts=1,
        )
