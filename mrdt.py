"""Mergeable Replicated Data Types for topology-scoped consistency.

Each tier maintains a local MRDT state. During connectivity, tiers
exchange and merge states. During blackout, tiers continue independently.
Merge is automatic and conflict-free when connectivity returns.

Implemented types:
- GCounter: grow-only counter (one slot per tier, merge = max per slot)
- PNCounter: increment/decrement counter (two G-Counters)
- LWWRegister: last-writer-wins register (timestamp-based)
- VersionVector: tracks causal ordering across tiers
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class GCounter:
    """Grow-only counter. Each tier has its own slot.

    Merge: take the max of each slot.
    Value: sum of all slots.
    """

    def __init__(self, tier_id: str, tier_ids: list[str]):
        self.tier_id = tier_id
        self._counts: dict[str, int] = {t: 0 for t in tier_ids}

    def increment(self, amount: int = 1):
        self._counts[self.tier_id] += amount

    def value(self) -> int:
        return sum(self._counts.values())

    def merge(self, other: GCounter) -> GCounter:
        """Merge two G-Counters. Returns self (mutated)."""
        for tid, count in other._counts.items():
            self._counts[tid] = max(self._counts.get(tid, 0), count)
        return self

    def clone(self) -> GCounter:
        g = GCounter(self.tier_id, [])
        g._counts = dict(self._counts)
        return g

    def __repr__(self):
        return f"GCounter({self.tier_id}, val={self.value()}, slots={self._counts})"


class PNCounter:
    """Positive-negative counter (increment and decrement).

    Two G-Counters: one for increments, one for decrements.
    Value: increments.value() - decrements.value()
    """

    def __init__(self, tier_id: str, tier_ids: list[str]):
        self.tier_id = tier_id
        self._pos = GCounter(tier_id, tier_ids)
        self._neg = GCounter(tier_id, tier_ids)

    def increment(self, amount: int = 1):
        self._pos.increment(amount)

    def decrement(self, amount: int = 1):
        self._neg.increment(amount)

    def value(self) -> int:
        return self._pos.value() - self._neg.value()

    def merge(self, other: PNCounter) -> PNCounter:
        self._pos.merge(other._pos)
        self._neg.merge(other._neg)
        return self

    def clone(self) -> PNCounter:
        pn = PNCounter(self.tier_id, [])
        pn._pos = self._pos.clone()
        pn._neg = self._neg.clone()
        return pn


class LWWRegister:
    """Last-writer-wins register. Timestamp resolves conflicts.

    Each write carries a timestamp. Merge keeps the highest timestamp.
    """

    def __init__(self, tier_id: str):
        self.tier_id = tier_id
        self._value: Any = None
        self._timestamp: float = 0.0
        self._writer: str = ""

    def write(self, value: Any, timestamp: float):
        if timestamp > self._timestamp:
            self._value = value
            self._timestamp = timestamp
            self._writer = self.tier_id

    def read(self) -> Any:
        return self._value

    def merge(self, other: LWWRegister) -> LWWRegister:
        if other._timestamp > self._timestamp:
            self._value = other._value
            self._timestamp = other._timestamp
            self._writer = other._writer
        return self

    def clone(self) -> LWWRegister:
        r = LWWRegister(self.tier_id)
        r._value = self._value
        r._timestamp = self._timestamp
        r._writer = self._writer
        return r

    def __repr__(self):
        return f"LWWRegister({self.tier_id}, val={self._value}, t={self._timestamp:.1f}, by={self._writer})"


class VersionVector:
    """Version vector for causal ordering across tiers."""

    def __init__(self, tier_id: str, tier_ids: list[str]):
        self.tier_id = tier_id
        self._versions: dict[str, int] = {t: 0 for t in tier_ids}

    def tick(self):
        self._versions[self.tier_id] += 1
        return dict(self._versions)

    def merge(self, other: VersionVector) -> VersionVector:
        for tid, ver in other._versions.items():
            self._versions[tid] = max(self._versions.get(tid, 0), ver)
        return self

    def dominates(self, other: VersionVector) -> bool:
        """Does self causally dominate other?"""
        return all(
            self._versions.get(t, 0) >= other._versions.get(t, 0)
            for t in set(self._versions) | set(other._versions)
        )

    def concurrent_with(self, other: VersionVector) -> bool:
        """Are self and other causally concurrent?"""
        return not self.dominates(other) and not other.dominates(self)

    def staleness(self, other: VersionVector) -> dict[str, int]:
        """How many updates behind is self relative to other, per tier?"""
        return {
            t: max(0, other._versions.get(t, 0) - self._versions.get(t, 0))
            for t in set(self._versions) | set(other._versions)
        }

    def clone(self) -> VersionVector:
        vv = VersionVector(self.tier_id, [])
        vv._versions = dict(self._versions)
        return vv

    def __repr__(self):
        return f"VV({self.tier_id}, {self._versions})"


@dataclass
class MergeEvent:
    """Record of a merge operation between tiers."""
    time: float
    src_tier: str
    dst_tier: str
    staleness: dict[str, int]  # per-tier staleness before merge
    merge_latency: float  # network delay for the merge
