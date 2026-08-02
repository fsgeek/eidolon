"""The capability gate must see partitions, not just links.

Pre-registration constraint A2
(docs/superpowers/notes/2026-07-29-midround-flip-preregistration.md):
`DatacenterNetwork.get_link` is partition-blind by design — it answers a
topology question.  Any reachable set built from it keeps certifying
nodes that packets can no longer reach, so a capability gate reading
get_link would keep asserting (1,1) after a mid-round flip and mislabel
every treatment row.

These tests pin the divergence and prove the fix detects a capability
change that the old predicate could not.
"""
import simpy

from capability import classify


def _leo_reach(sys_, predicate):
    return {a for a in sys_.all_ids if predicate(sys_.leo_prop.entity.id, a)}


def test_get_link_is_partition_blind_and_is_reachable_is_not():
    """The two predicates agree until a partition exists, then diverge.

    Built through wire_duel because five_dc_topology alone registers no
    entities — a bare topology makes every entity-keyed assertion below
    vacuous.
    """
    from duel import wire_duel

    env = simpy.Environment()
    sys_ = wire_duel(env, k=5, polarity="leo_high", earth_max_rounds=1,
                     leo_max_rounds=8, jitter_scale=0.0, seed=0)
    net = sys_.network

    src = sys_.leo_prop.entity.id
    a = net._entity_location[src]
    dst = next(i for i in sys_.all_ids
               if net._entity_location[i] != a
               and net.get_link(src, i) is not None)
    b = net._entity_location[dst]
    assert a != b and net.has_link(a, b) or net.has_link(b, a)

    # Before: both predicates agree.
    assert net.get_link(src, dst) is not None
    assert net.is_reachable(src, dst) is True

    net.partition_locations(a, b)

    # After: get_link is unchanged — this is the defect, pinned.
    assert net.get_link(src, dst) is not None, (
        "get_link is intentionally topology-only; if this ever changes, "
        "is_reachable's composition must be revisited")
    # ...and is_reachable reflects deliverability.
    assert net.is_reachable(src, dst) is False

    net.heal_locations(a, b)
    assert net.is_reachable(src, dst) is True


def test_gate_detects_partition_induced_capability_loss():
    """A partition that severs LEO from Earth must flip the certified state.

    This is the A2 verification: with the old get_link predicate the
    report is unchanged by the partition, so the gate would certify a
    regime the trial is no longer in.
    """
    from duel import wire_duel

    env = simpy.Environment()
    sys_ = wire_duel(env, k=3, polarity="earth_high", earth_max_rounds=1,
                     leo_max_rounds=8, jitter_scale=0.0, seed=0)
    net = sys_.network

    before = classify(sys_.wall, 2, _leo_reach(sys_, net.is_reachable))
    assert before.r1 and before.r2, "k=3 wires LEO as a (1,1) failover peer"

    leo_loc = net._entity_location[sys_.leo_prop.entity.id]
    earth_locs = {net._entity_location[a] for a in sys_.all_ids
                  if net._entity_location[a] != leo_loc}
    for loc in earth_locs:
        net.partition_locations(leo_loc, loc)

    after = classify(sys_.wall, 2, _leo_reach(sys_, net.is_reachable))
    assert not after.r1 and not after.r2, (
        f"severing LEO from every other location must lose both "
        f"capabilities; got ({after.r1},{after.r2})")

    # The old predicate cannot see it — the reason A2 is a blocking
    # constraint rather than a cosmetic fix.
    stale = classify(
        sys_.wall, 2,
        _leo_reach(sys_, lambda s, d: net.get_link(s, d) is not None))
    assert stale.r1 and stale.r2, (
        "regression canary: if get_link ever becomes partition-aware, "
        "this assertion fires and the A2 rationale needs rewriting")
