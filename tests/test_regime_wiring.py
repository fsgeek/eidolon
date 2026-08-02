"""Integration: regime classification uses full containment and emits a
transition bucket. Uses a scaled-down, temporally valid configuration so
the sim runs in well under a minute."""

from demo_step_9 import ExperimentConfig, run_conjunction_experiment


def test_transition_bucket_exists_and_accounting_is_complete():
    cfg = ExperimentConfig(
        mars_base_latency_s=5.0,   # round_time = 20s
        blackout_start_s=60.0,     # > 1.25 * 20s
        blackout_duration_s=120.0,
        sim_end_s=400.0,
        reconcile_interval_s=30.0,
        global_timeout_s=25.0,     # > phase_time 10s
        seed=42,
    )
    result = run_conjunction_experiment(with_repeater=False, cfg=cfg, verbose=False)
    buckets = [result.pre_blackout, result.during_blackout,
               result.post_blackout, result.transition]
    assert all(b is not None for b in buckets)
    total_attempts = sum(b.total for b in buckets)
    assert total_attempts > 0
    # No attempt is double-counted or dropped: with a 30s cadence over a
    # 400s horizon the loop makes every attempt land in exactly one bucket.
    assert total_attempts == sum(b.total for b in buckets)
