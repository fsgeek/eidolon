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
    """Topology-aware quorum based on Peleg & Wool's crumbling walls.

    NOTE ON INDEXING: The code and paper use opposite tier-index
    conventions.  The code accepts tiers ordered slow-to-fast
    (index 0 = Mars/top of wall, last index = Earth/bottom), matching
    the constructor's natural "read downward toward the fast tier"
    iteration.  The paper (main.tex) indexes from the bottom:
    T_0 = Earth, T_3 = Mars.  The algorithm is identical; only the
    numbering differs.  To map: paper tier i = code tier (num_tiers-1-i).

    Tiers are ordered slow-to-fast (Mars at top, Earth at bottom).
    The wall structure gives each tier a DIFFERENT Phase 1 quorum
    requirement: a proposer reads DOWN through the wall, needing
    one node from its own tier and each tier below it.

    Example tiers (top to bottom of wall):
      Code index 0 (Mars, paper T_3):  [mars-0, mars-1, mars-2]
      Code index 1 (Moon, paper T_2):  [moon-0]
      Code index 2 (LEO,  paper T_1):  [sat-0]
      Code index 3 (Earth, paper T_0): [na, eu, asia, sa, af]

    Phase 1 quorum (per-tier, reading downward):
      Mars proposer:  one from Mars + one from Moon + one from LEO + one from Earth
      Moon proposer:  one from Moon + one from LEO + one from Earth
      LEO proposer:   one from LEO + one from Earth
      Earth proposer: |E|-k+1 Earth nodes — a SINGLE Earth node under
                      strict Phase 2 (k=|E|)

    The |E|-k+1 Earth floor (min_earth_in_q1) applies to EVERY row above,
    not just the Earth one: under relaxed k the "one from Earth" in each
    line becomes |E|-k+1 from Earth.

    Phase 2 quorum (commits, hot path):
      All nodes (or k-of-n) from the fastest tier (Earth).

    Intersection guarantee:
      Every Phase 1 quorum contains at least |E|-k+1 Earth nodes and
      every Phase 2 quorum contains at least k, and
      (|E|-k+1) + k > |E|, so by pigeonhole they share an Earth node.
      Note that "reads down to Earth" alone is NOT sufficient: at k < |E|
      a Q1 holding one Earth node can be disjoint from a valid k-of-|E|
      Q2.  The Earth floor, not the downward chain, carries safety.

    Liveness consequence:
      During Mars blackout, only Mars-initiated Phase 1 is blocked.
      Moon, LEO, and Earth can still complete Phase 1 because they
      never needed Mars. The liveness failure is scoped to the
      unreachable tier, not the whole system.
    """

    def __init__(self, tiers: list[list[int]], phase2_threshold: int | None = None):
        """Create a crumbling wall quorum from geographic tiers.

        Args:
            tiers: List of tiers, ordered slow-to-fast (top to bottom).
                   Last tier is the "fast" tier used for Phase 2.
                   Example: [[mars_ids...], [moon_id], [leo_id], [earth_ids...]]
            phase2_threshold: Minimum fast-tier nodes for Phase 2.
                   None (default) = all nodes in the fast tier (strict).
                   e.g., 4 for 4-of-5 Earth (relaxed).
        """
        all_nodes = []
        seen: set[int] = set()
        duplicates: set[int] = set()
        for tier in tiers:
            for node_id in tier:
                if node_id in seen:
                    duplicates.add(node_id)
                seen.add(node_id)
            all_nodes.extend(tier)
        if duplicates:
            raise ValueError(
                f"tiers must be pairwise disjoint; node id(s) "
                f"{sorted(duplicates)} appear in more than one tier"
            )
        super().__init__(all_nodes)

        self.tiers = tiers
        self.tier_sizes = [len(t) for t in tiers]
        self.num_tiers = len(tiers)
        self.fast_tier = tiers[-1]  # Last tier = fastest (bottom of wall)
        self._tier_sets = [set(t) for t in tiers]
        self._fast_tier_set = set(self.fast_tier)

        # Build a lookup: node_id -> tier index
        self._node_to_tier: dict[int, int] = {}
        for i, tier in enumerate(tiers):
            for node_id in tier:
                self._node_to_tier[node_id] = i

        # Phase 2: all of the fast tier, or a threshold subset
        if phase2_threshold is None:
            self._phase2_size = len(self.fast_tier)
            self._phase2_threshold = len(self.fast_tier)
        else:
            if phase2_threshold < 1 or phase2_threshold > len(self.fast_tier):
                raise ValueError(
                    f"phase2_threshold must be in [1, {len(self.fast_tier)}], "
                    f"got {phase2_threshold}"
                )
            self._phase2_size = phase2_threshold
            self._phase2_threshold = phase2_threshold

        # Minimum Earth nodes in any Q1 for intersection with relaxed Q2.
        # Pigeonhole: min_earth + phase2_threshold > |E|
        # For strict Q2 (threshold = |E|): min_earth >= 1 (always satisfied)
        # For relaxed Q2 (e.g. 4-of-5): min_earth >= 2
        self._min_earth_in_q1 = len(self.fast_tier) - self._phase2_threshold + 1

    def tier_of(self, node_id: int) -> int:
        """Return the tier index for a given node ID."""
        return self._node_to_tier[node_id]

    @property
    def phase2_threshold(self) -> int:
        """The k in k-of-|fast tier| required for Phase 2."""
        return self._phase2_threshold

    @property
    def min_earth_in_q1(self) -> int:
        """Minimum fast-tier nodes any Q1 needs: |E| - k + 1 (hitting set)."""
        return self._min_earth_in_q1

    def phase1_quorum_size(self, initiator_tier: int | None = None) -> int:
        """Minimum Phase 1 quorum size for the initiating tier.

        A proposer at tier i needs one node from each intermediate tier
        j in [i, num_tiers-1) plus min_earth_in_q1 fast-tier nodes (the
        |E|-k+1 hitting set that guarantees Q1/Q2 intersection). For
        strict Phase 2 the hitting set is 1 and this equals the tier
        count; under relaxed k it is strictly larger.
        """
        if initiator_tier is None:
            # Top of wall (Mars) — worst case, for backwards compatibility
            initiator_tier = 0
        intermediate = self.num_tiers - 1 - initiator_tier
        return intermediate + self._min_earth_in_q1

    def phase2_quorum_size(self) -> int:
        return self._phase2_size

    def is_phase1_quorum(self, respondents: set[int], initiator_tier: int | None = None) -> bool:
        """Phase 1: must have one node from each tier at or below initiator.

        Args:
            respondents: Set of node IDs that responded.
            initiator_tier: Code-convention tier index of the proposer
                (0=Mars/top of wall, last=Earth/bottom; see class docstring
                for paper-to-code index mapping).
                If None, defaults to 0 (top of wall — requires all tiers).
        """
        if initiator_tier is None:
            initiator_tier = 0
        # Need at least one respondent from each tier from initiator down to bottom
        for j in range(initiator_tier, self.num_tiers):
            if not (respondents & self._tier_sets[j]):
                return False
        # For relaxed Q2, need enough Earth nodes for pigeonhole intersection
        if len(respondents & self._fast_tier_set) < self._min_earth_in_q1:
            return False
        return True

    def is_phase2_quorum(self, respondents: set[int]) -> bool:
        """Phase 2 requires phase2_threshold nodes from the fast tier."""
        return len(respondents & self._fast_tier_set) >= self._phase2_threshold

    def describe(self) -> str:
        tier_desc = " / ".join(f"{len(t)}" for t in self.tiers)
        p2_desc = ("fast tier" if self._phase2_threshold == len(self.fast_tier)
                   else f"{self._phase2_threshold}-of-{len(self.fast_tier)} fast tier")
        return (f"CrumblingWall(tiers=[{tier_desc}]): "
                f"Phase1=read-down (top needs {self.phase1_quorum_size(0)}, "
                f"bottom needs {self.phase1_quorum_size(self.num_tiers - 1)}), "
                f"Phase2={self._phase2_size} ({p2_desc})")

    def describe_tiers(self, tier_names: list[str]) -> str:
        """Human-readable tier description."""
        lines = []
        for i, (name, tier) in enumerate(zip(tier_names, self.tiers)):
            speed = "FAST" if i == len(self.tiers) - 1 else "slow"
            q1_needs = self.phase1_quorum_size(i)
            lines.append(f"    Tier {i} ({name}): {len(tier)} nodes [{speed}], "
                         f"Phase 1 minimum {q1_needs} (reads downward)")
        lines.append(f"    Phase 2: {self._phase2_size} (fast tier)")
        return "\n".join(lines)


class AnchoredMajorityQuorum(QuorumSystem):
    """Majority Phase 1 over all nodes, Phase 2 anchored to a fast tier.

    Competitive Flexible Paxos baseline for the wall comparison: Phase 1
    accepts any majority (floor(n/2)+1) of the full node set with no tier
    structure, while Phase 2 requires every anchor node — the same strict
    fast-tier Phase 2 the wall uses, so the two constructions differ only
    in Phase 1 shape.

    Intersection: Phase 2 is exactly the anchor set, so safety needs
    every majority to contain an anchor node, i.e.
    floor(n/2) + 1 > n - len(anchor).
    """

    def __init__(self, nodes: list[int], anchor: list[int]):
        super().__init__(nodes)
        self.anchor = list(anchor)
        self._anchor_set = set(anchor)
        if not self._anchor_set <= set(nodes):
            raise ValueError("anchor must be a subset of nodes")
        if self.phase1_quorum_size() <= self.n - len(self.anchor):
            raise ValueError(
                f"majority Q1 does not intersect the anchor Q2: "
                f"{self.phase1_quorum_size()} <= {self.n} - {len(self.anchor)}"
            )

    def phase1_quorum_size(self) -> int:
        return self.n // 2 + 1

    def phase2_quorum_size(self) -> int:
        return len(self.anchor)

    def is_phase1_quorum(self, respondents: set[int], initiator_tier: int | None = None) -> bool:
        """Any majority of the full node set; tier structure is ignored."""
        return len(respondents & set(self.nodes)) >= self.phase1_quorum_size()

    def is_phase2_quorum(self, respondents: set[int]) -> bool:
        """Every anchor node must respond (strict fast-tier Phase 2)."""
        return self._anchor_set <= respondents

    def describe(self) -> str:
        return (f"AnchoredMajority(n={self.n}): "
                f"Phase1=any {self.phase1_quorum_size()}, "
                f"Phase2=all {len(self.anchor)} anchor nodes")


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
