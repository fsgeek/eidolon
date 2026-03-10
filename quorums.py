"""Quorum system constructions beyond simple majority.

Peleg & Wool (1995) "The Availability of Quorum Systems" showed
that quorum systems don't require majorities. Various combinatorial
constructions achieve smaller quorum sizes while preserving the
intersection property that consensus needs.

Key insight: ANY two quorums must share at least one member.
Majority guarantees this trivially (pigeonhole), but it's
sufficient, not necessary.

Constructions implemented:
- Majority: floor(n/2) + 1 (baseline)
- Grid: arrange nodes in sqrt(n) x sqrt(n) grid,
  quorum = full column + one from each row. Size ≈ 2*sqrt(n).
- Paths (crumbling walls): even more aggressive constructions

Combined with Flexible Paxos, these become very powerful:
- Phase 1 can use a large, slow quorum (rare)
- Phase 2 can use a small, fast quorum (every commit)
- The two just need to intersect
"""

import math
from paxos import QuorumSystem


class GridQuorum(QuorumSystem):
    """Grid quorum (Cheung, Ammar, Ahamad 1990).

    Arrange n nodes in a rows x cols grid.
    Quorum = one full row + one element from each other row.
    Size = cols + (rows - 1) ≈ 2*sqrt(n) for square grids.

    For non-square n, we find the best rectangular arrangement.

    Intersection property: any two quorums share at least one
    element (both contain a full row, and at least one element
    from every other row - pigeonhole on columns).

    Actually, the standard grid quorum is:
    Q = full column + one per remaining column's row
    Let me use the standard: a row + one from each other row.
    Two such quorums: Q1 has row i, Q2 has row j.
    Q1 includes one element from row j. Q2 includes one element from row i.
    Wait - do they intersect?

    Standard grid quorum: pick a full COLUMN (all rows in that column)
    plus one element from each remaining column.
    Size = rows + (cols - 1) = rows + cols - 1.

    Two quorums Q1, Q2: Q1 has full column c1, Q2 has full column c2.
    Q1 also has one element from column c2 (in some row r).
    Q2 has all of column c2, so it has the element in (r, c2).
    But Q1's element from c2 is (r, c2). Is (r, c2) in Q2?
    Q2 has full column c2, so yes! (r, c2) is in both.

    Intersection guaranteed.
    """

    def __init__(self, nodes: list[int], rows: int = 0, cols: int = 0):
        super().__init__(nodes)
        if rows == 0 or cols == 0:
            # Auto-compute best grid dimensions
            rows, cols = self._best_grid(self.n)
        self.rows = rows
        self.cols = cols
        # Arrange nodes into grid (row-major)
        self._grid = []
        idx = 0
        for r in range(rows):
            row = []
            for c in range(cols):
                if idx < self.n:
                    row.append(self.nodes[idx])
                    idx += 1
            self._grid.append(row)

    @staticmethod
    def _best_grid(n: int) -> tuple[int, int]:
        """Find the most square-like grid for n nodes."""
        best_rows = 1
        best_cols = n
        for r in range(1, int(math.sqrt(n)) + 2):
            c = math.ceil(n / r)
            if r * c >= n and abs(r - c) < abs(best_rows - best_cols):
                best_rows, best_cols = r, c
        return best_rows, best_cols

    def quorum_size(self) -> int:
        """Size of a grid quorum: rows + cols - 1."""
        return self.rows + self.cols - 1

    def phase1_quorum_size(self) -> int:
        return self.quorum_size()

    def phase2_quorum_size(self) -> int:
        return self.quorum_size()

    def describe(self) -> str:
        return (f"Grid({self.rows}x{self.cols}): "
                f"quorum={self.quorum_size()} of {self.n}")


class FlexibleGridQuorum(QuorumSystem):
    """Grid quorum combined with Flexible Paxos.

    Phase 1: full grid quorum (for safety during elections)
    Phase 2: smaller quorum (for speed during commits)

    The intersection between Phase 1 and Phase 2 quorums
    is guaranteed by construction as long as Phase 2 selects
    at least one node from every column of the grid.
    """

    def __init__(self, nodes: list[int], phase2_size: int,
                 rows: int = 0, cols: int = 0):
        super().__init__(nodes)
        if rows == 0 or cols == 0:
            rows, cols = GridQuorum._best_grid(self.n)
        self.rows = rows
        self.cols = cols
        self._phase2_size = phase2_size

        # Phase 1 uses full grid quorum
        self._phase1_size = rows + cols - 1

        # Verify intersection: phase1 + phase2 > n
        if self._phase1_size + self._phase2_size <= self.n:
            raise ValueError(
                f"Flexible quorum requires q1 + q2 > n: "
                f"{self._phase1_size} + {self._phase2_size} <= {self.n}"
            )

    def phase1_quorum_size(self) -> int:
        return self._phase1_size

    def phase2_quorum_size(self) -> int:
        return self._phase2_size


class CrumblingWallQuorum(QuorumSystem):
    """Topology-aware quorum: rows = geographic tiers, columns = nodes.

    Peleg & Wool's crumbling walls generalized to non-rectangular grids.
    Each tier (row) has a different number of nodes. The topology itself
    defines the quorum structure.

    Example tiers:
      Row 0 (Mars):    [mars-0, mars-1, mars-2]  (slow, far)
      Row 1 (Moon):    [moon-0]                   (slower)
      Row 2 (LEO):     [sat-0]                    (medium)
      Row 3 (Earth):   [na, eu, asia, sa, af]     (fast, close)

    Phase 1 quorum (elections, rare):
      One node from EACH tier → geographic spread for durability.
      Size = number of tiers (e.g., 4).
      Requires: at least one node from every tier.

    Phase 2 quorum (commits, hot path):
      All nodes from the FASTEST tier (Earth).
      Size = len(fastest_tier).
      Stays entirely on the fast path.

    Intersection guarantee:
      Phase 1 includes one from the fastest tier.
      Phase 2 includes ALL of the fastest tier.
      Therefore Phase 1 ∩ Phase 2 ≠ ∅.

    Combined with Flexible Paxos: q1 + q2 > n required.
    If tiers are [3, 1, 1, 5] = 10 total,
    Phase 1 = one per tier = 4, Phase 2 = all Earth = 5.
    4 + 5 = 9 ≤ 10. NOT enough! Need q1 ≥ 6.
    Fix: Phase 1 = one per tier + extras from Earth.

    The real construction: Phase 1 spans all tiers AND is large enough
    that q1 + q2 > n. Phase 2 is the fast tier.
    """

    def __init__(self, tiers: list[list[int]]):
        """Create a crumbling wall quorum from geographic tiers.

        Args:
            tiers: List of tiers, ordered slow-to-fast.
                   Last tier is the "fast" tier used for Phase 2.
                   Example: [[mars_ids...], [moon_id], [leo_id], [earth_ids...]]
        """
        all_nodes = []
        for tier in tiers:
            all_nodes.extend(tier)
        super().__init__(all_nodes)

        self.tiers = tiers
        self.tier_sizes = [len(t) for t in tiers]
        self.num_tiers = len(tiers)
        self.fast_tier = tiers[-1]  # Last tier = fastest
        self._tier_sets = [set(t) for t in tiers]
        self._fast_tier_set = set(self.fast_tier)

        # Phase 2: all of the fast tier
        self._phase2_size = len(self.fast_tier)

        # Phase 1: must satisfy q1 + q2 > n AND span all tiers
        # Minimum: one from each tier = num_tiers
        # Required: q1 > n - q2 = n - len(fast_tier)
        min_phase1 = self.n - self._phase2_size + 1
        # Also must have at least one from each tier
        min_from_tiers = self.num_tiers
        self._phase1_size = max(min_phase1, min_from_tiers)

        # How many EXTRA nodes needed beyond one-per-tier?
        self._extras_needed = self._phase1_size - self.num_tiers
        # Extras come from the fast tier (cheapest to add)
        self._extras_from_fast = min(self._extras_needed, len(self.fast_tier) - 1)

    def phase1_quorum_size(self) -> int:
        return self._phase1_size

    def phase2_quorum_size(self) -> int:
        return self._phase2_size

    def is_phase1_quorum(self, respondents: set[int]) -> bool:
        """Phase 1 must span all tiers and meet minimum size."""
        covered_tiers = 0
        for tier in self._tier_sets:
            if respondents & tier:
                covered_tiers += 1
        return (
            covered_tiers == self.num_tiers
            and len(respondents & set(self.nodes)) >= self._phase1_size
        )

    def is_phase2_quorum(self, respondents: set[int]) -> bool:
        """Phase 2 is the full fast tier (Earth in our examples)."""
        return self._fast_tier_set.issubset(respondents)

    def describe(self) -> str:
        tier_desc = " / ".join(f"{len(t)}" for t in self.tiers)
        return (f"CrumblingWall(tiers=[{tier_desc}]): "
                f"Phase1={self._phase1_size} (span all tiers), "
                f"Phase2={self._phase2_size} (fast tier)")

    def describe_tiers(self, tier_names: list[str]) -> str:
        """Human-readable tier description."""
        lines = []
        for i, (name, tier) in enumerate(zip(tier_names, self.tiers)):
            speed = "FAST" if i == len(self.tiers) - 1 else "slow"
            lines.append(f"    Tier {i} ({name}): {len(tier)} nodes [{speed}]")
        lines.append(f"    Phase 1: {self._phase1_size} "
                     f"(one per tier + {self._extras_needed} extras from fast tier)")
        lines.append(f"    Phase 2: {self._phase2_size} (all of fast tier)")
        return "\n".join(lines)


def compare_quorum_sizes():
    """Show how quorum sizes scale with n."""
    print(f"  {'n':>4}  {'Majority':>10}  {'Grid':>10}  {'Grid dims':>12}")
    print(f"  {'─'*4}  {'─'*10}  {'─'*10}  {'─'*12}")
    for n in [3, 4, 5, 7, 9, 16, 25, 49, 100]:
        majority = n // 2 + 1
        rows, cols = GridQuorum._best_grid(n)
        grid = rows + cols - 1
        print(f"  {n:>4}  {majority:>10}  {grid:>10}  {rows}x{cols:>9}")


if __name__ == "__main__":
    print("Quorum size scaling: Majority vs Grid")
    print()
    compare_quorum_sizes()
    print()
    print("Crumbling Wall example: Mars + Moon + LEO + Earth")
    print()
    wall = CrumblingWallQuorum([
        [100, 101, 102],  # Mars (3 nodes)
        [200],            # Moon (1 node)
        [300],            # LEO (1 node)
        [1, 2, 3, 4, 5], # Earth (5 nodes)
    ])
    print(wall.describe())
    print()
    print(wall.describe_tiers(["Mars", "Moon", "LEO", "Earth"]))
