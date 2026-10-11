"""Sanity checks for the Caerus merger model (not the registered predictions)."""

from caerus_model import Config, Merger, Txn, run, unaffected_goodput


def _txn(tid, items):
    return Txn(tid, items[0][0], 0.0, tuple(items))


def test_cycle_across_partial_sequences_releases_as_one_scc():
    m = Merger()
    a = _txn(1, [(0, 0), (1, 0)])
    b = _txn(2, [(1, 0), (0, 0)])
    # Region 0 sequences a before b; region 1 sequences b before a.
    m.insert(0, a)
    m.insert(0, b)
    m.insert(1, b)
    m.insert(1, a)
    assert b.out == {1} and a.out == {2}
    assert m.release(1.0) == 2


def test_incomplete_predecessor_blocks_successor():
    m = Merger()
    x = _txn(1, [(0, 0), (1, 0)])  # never seen in region 1's sequence
    y = _txn(2, [(0, 0)])
    m.insert(0, x)
    m.insert(0, y)
    assert m.release(1.0) == 0
    assert y.out == {1}


def test_released_predecessor_adds_no_edge():
    m = Merger()
    x = _txn(1, [(0, 0)])
    m.insert(0, x)
    assert m.release(1.0) == 1
    y = _txn(2, [(0, 0)])
    m.insert(0, y)
    assert y.out == set()


def test_no_failure_everything_releases():
    cfg = Config(p_mr=0.3, dead=(), horizon=3.0, rate=500.0)
    txns = run(cfg)
    assert all(x.released_t is not None for x in txns)


def test_single_region_workload_unaffected_by_failure():
    cfg = Config(p_mr=0.0, horizon=4.0, t_fail=1.0, rate=500.0)
    txns = run(cfg)
    assert all(g == 1.0 for _, g in unaffected_goodput(txns, cfg))
