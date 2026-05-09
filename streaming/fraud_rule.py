"""Pure-function velocity fraud rule.

This module owns the BUSINESS LOGIC of the velocity-based fraud rule and
NOTHING ELSE. It holds no state and performs no I/O. Same input → same
output, every time.

Rationale (design note §12): keeping the rule as a pure function over an
already-windowed list of transactions:

  - makes unit testing trivial (no mocks, no fixtures, no docker)
  - prevents hidden state bugs in the rule logic
  - means the eventual Phase 5+ migration to Spark Structured Streaming or
    Flink only has to replace the windowing/state machinery — this function
    survives unchanged.

The CONSUMER owns:
  - the per-customer deque of recent transactions
  - filtering that deque to the K-second processing-time window
  - I/O to Kafka and Postgres

The RULE (this module) owns:
  - deciding FLAG vs PASS given an already-windowed list and thresholds
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional

from streaming.config import VelocityThresholds


@dataclass(frozen=True)
class Transaction:
    """Lightweight projection of a transaction for rule evaluation.

    `processing_time` is the wall-clock epoch-seconds at which the consumer
    received the message — not the upstream event time. We use processing
    time because PaySim transactions don't carry meaningful wall-clock
    timestamps; see design note §4.
    """
    transaction_id: str
    name_orig: str
    amount: float
    processing_time: float


@dataclass(frozen=True)
class Decision:
    """Outcome of a single rule evaluation."""
    action: Literal["FLAG", "PASS"]
    rule_name: str = ""
    evidence: Optional[dict] = None


def evaluate_velocity_rule(
    recent_transactions: list[Transaction],
    thresholds: VelocityThresholds,
) -> Decision:
    """Decide whether the velocity rule fires for ONE customer.

    Flags iff BOTH:
      - len(recent_transactions) > thresholds.n   (strict greater)
      - sum(t.amount) > thresholds.t_amount       (strict greater)

    Args:
        recent_transactions: Transactions for a SINGLE customer, already
            windowed to the last K seconds by the caller. This function does
            not look at processing_time itself — windowing is the consumer's
            job (see design note §12).
        thresholds: Frozen VelocityThresholds.

    Returns:
        Decision(action="FLAG", rule_name="velocity_breach", evidence={...})
        or Decision(action="PASS").
    """
    count = len(recent_transactions)
    total = float(sum(t.amount for t in recent_transactions))

    if count > thresholds.n and total > thresholds.t_amount:
        return Decision(
            action="FLAG",
            rule_name="velocity_breach",
            evidence={
                "recent_count": count,
                "total_amount": total,
                "time_window_seconds": thresholds.k_seconds,
                "threshold_n": thresholds.n,
                "threshold_t": float(thresholds.t_amount),
            },
        )
    return Decision(action="PASS")
