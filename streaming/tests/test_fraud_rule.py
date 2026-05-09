"""Unit tests for the pure-function velocity fraud rule.

These tests are pure: no Kafka, no Postgres, no docker, no fixtures.
They run in milliseconds and gate every commit via `make test-unit`.
"""
from __future__ import annotations

import dataclasses

import pytest

from streaming.config import VelocityThresholds
from streaming.fraud_rule import Decision, Transaction, evaluate_velocity_rule


T = VelocityThresholds()  # defaults: n=5, k_seconds=60, t_amount=100_000


def _txns(n: int, amount: float, t0: float = 1_700_000_000.0,
          customer: str = "C1") -> list[Transaction]:
    """Build a list of n transactions for one customer, 1 second apart."""
    return [
        Transaction(
            transaction_id=f"txn_{customer}_{i}",
            name_orig=customer,
            amount=amount,
            processing_time=t0 + i,
        )
        for i in range(n)
    ]


def test_velocity_under_threshold_passes():
    """4 transactions with sum > T should not flag — count is the gate."""
    # sum = 200_000 > T(100_000), but count = 4 < N(5)
    decision = evaluate_velocity_rule(_txns(4, amount=50_000), T)
    assert decision.action == "PASS"


def test_velocity_at_threshold_passes():
    """Exactly 5 transactions should not flag — strict > on count."""
    # count == N, not > N
    decision = evaluate_velocity_rule(_txns(5, amount=50_000), T)
    assert decision.action == "PASS"


def test_velocity_over_threshold_count_only():
    """6 transactions with sum < T should not flag — amount is the gate."""
    # count = 6 > N, but sum = 60_000 < T
    decision = evaluate_velocity_rule(_txns(6, amount=10_000), T)
    assert decision.action == "PASS"


def test_velocity_over_both_thresholds_flags():
    """6 transactions with sum > T should flag with rule_name=velocity_breach."""
    # count = 6 > N AND sum = 150_000 > T
    decision = evaluate_velocity_rule(_txns(6, amount=25_000), T)
    assert decision.action == "FLAG"
    assert decision.rule_name == "velocity_breach"


def test_empty_history_passes():
    """Edge case: no prior transactions."""
    assert evaluate_velocity_rule([], T).action == "PASS"


def test_flag_includes_rule_details():
    """Flag evidence dict contains the full set of expected fields."""
    decision = evaluate_velocity_rule(_txns(7, amount=30_000), T)
    assert decision.action == "FLAG"
    assert decision.evidence is not None
    assert decision.evidence["recent_count"] == 7
    assert decision.evidence["total_amount"] == pytest.approx(210_000)
    assert decision.evidence["time_window_seconds"] == 60
    assert decision.evidence["threshold_n"] == 5
    assert decision.evidence["threshold_t"] == pytest.approx(100_000)


def test_pure_function_determinism():
    """Same input → same output, every time. No hidden state."""
    txns = _txns(6, amount=25_000)
    d1 = evaluate_velocity_rule(txns, T)
    d2 = evaluate_velocity_rule(txns, T)
    d3 = evaluate_velocity_rule(txns, T)
    assert d1 == d2 == d3


def test_thresholds_dataclass_immutable():
    """Frozen dataclass — can't be mutated mid-evaluation."""
    with pytest.raises(dataclasses.FrozenInstanceError):
        T.n = 99  # type: ignore[misc]
