from __future__ import annotations

import pytest

from app.lookback_eval import CostModel, buy_with_costs, max_drawdown, sell_with_costs


def test_buy_with_costs_zero_fee_zero_slip() -> None:
    cost = CostModel(fee_bps=0.0, slippage_bps=0.0)
    qty, leg = buy_with_costs(quote_amount=100.0, price=10.0, cost=cost)
    assert qty == pytest.approx(10.0)
    assert leg.fee_paid == pytest.approx(0.0)
    assert leg.slippage_paid == pytest.approx(0.0)


def test_sell_with_costs_zero_fee_zero_slip() -> None:
    cost = CostModel(fee_bps=0.0, slippage_bps=0.0)
    proceeds, leg = sell_with_costs(base_qty=2.0, price=10.0, cost=cost)
    assert proceeds == pytest.approx(20.0)
    assert leg.fee_paid == pytest.approx(0.0)
    assert leg.slippage_paid == pytest.approx(0.0)


def test_buy_with_costs_fee_and_slip_reduce_qty() -> None:
    cost = CostModel(fee_bps=10.0, slippage_bps=5.0)  # 0.10% fee, 0.05% slippage
    qty, leg = buy_with_costs(quote_amount=1000.0, price=100.0, cost=cost)

    # No-cost quantity would be 10. Both fee and slippage should reduce it.
    assert qty < 10.0
    assert leg.fee_paid > 0.0
    assert leg.slippage_paid > 0.0


def test_max_drawdown_examples() -> None:
    assert max_drawdown([]) == 0.0
    assert max_drawdown([1.0, 2.0, 3.0]) == 0.0

    dd = max_drawdown([100.0, 120.0, 110.0, 90.0, 130.0])
    # Worst drawdown is from 120 -> 90 = -25%
    assert dd == pytest.approx(-0.25)

