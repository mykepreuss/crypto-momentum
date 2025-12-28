from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CostModel:
    fee_bps: float
    slippage_bps: float

    @property
    def fee_rate(self) -> float:
        return float(self.fee_bps) / 10_000.0

    @property
    def slippage_rate(self) -> float:
        return float(self.slippage_bps) / 10_000.0


@dataclass(frozen=True)
class TradeLegCost:
    fee_paid: float
    slippage_paid: float


def buy_with_costs(
    quote_amount: float,
    *,
    price: float,
    cost: CostModel,
) -> tuple[float, TradeLegCost]:
    """
    Buy base using quote currency.

    Assumptions (simple but explicit):
    - Slippage is modeled by paying a worse price: price * (1 + slippage_rate)
    - Fee is modeled as additional quote paid on top of notional (quote-denominated fee)

    Returns:
      (base_qty, costs)
    """
    if quote_amount < 0.0:
        raise ValueError("quote_amount must be >= 0")
    if price <= 0.0:
        raise ValueError("price must be > 0")

    fee_rate = cost.fee_rate
    slip_rate = cost.slippage_rate

    effective_price = price * (1.0 + slip_rate)
    notional = quote_amount / (1.0 + fee_rate) if fee_rate > 0.0 else quote_amount
    fee_paid = quote_amount - notional
    base_qty = notional / effective_price

    slippage_paid = base_qty * (effective_price - price)
    return base_qty, TradeLegCost(fee_paid=fee_paid, slippage_paid=slippage_paid)


def sell_with_costs(
    base_qty: float,
    *,
    price: float,
    cost: CostModel,
) -> tuple[float, TradeLegCost]:
    """
    Sell base into quote currency.

    Assumptions:
    - Slippage is modeled by receiving a worse price: price * (1 - slippage_rate)
    - Fee is modeled as a % of gross quote proceeds (quote-denominated fee)

    Returns:
      (quote_amount, costs)
    """
    if base_qty < 0.0:
        raise ValueError("base_qty must be >= 0")
    if price <= 0.0:
        raise ValueError("price must be > 0")

    fee_rate = cost.fee_rate
    slip_rate = cost.slippage_rate

    effective_price = price * (1.0 - slip_rate)
    gross_proceeds = base_qty * effective_price
    fee_paid = gross_proceeds * fee_rate
    quote_amount = gross_proceeds - fee_paid

    slippage_paid = base_qty * (price - effective_price)
    return quote_amount, TradeLegCost(fee_paid=fee_paid, slippage_paid=slippage_paid)


def max_drawdown(equity: list[float]) -> float:
    """
    Returns max drawdown as a negative fraction (e.g. -0.12 for -12%).
    """
    if not equity:
        return 0.0

    peak = equity[0]
    worst = 0.0
    for v in equity:
        if v > peak:
            peak = v
        if peak > 0.0:
            dd = (v - peak) / peak
            if dd < worst:
                worst = dd
    return worst

