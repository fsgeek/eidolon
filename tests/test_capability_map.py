"""The capability-map experiment reproduces the spec's expected states."""

from experiments.capability_map import run


def _index(rows):
    return {(r["scenario"], r["initiator_tier"]): r for r in rows}


def test_map_contains_relaxation_sequence():
    rows = _index(run())
    assert (rows[("sparse_leo_k5", "leo")]["r1"],
            rows[("sparse_leo_k5", "leo")]["r2"]) == (1, 0)
    assert (rows[("sparse_leo_k4", "leo")]["r1"],
            rows[("sparse_leo_k4", "leo")]["r2"]) == (1, 0)
    assert (rows[("sparse_leo_k3", "leo")]["r1"],
            rows[("sparse_leo_k3", "leo")]["r2"]) == (1, 1)


def test_map_labels_hazards_and_authority():
    rows = _index(run())
    assert rows[("sparse_leo_k5", "leo")]["hazards"] == "acquire-without-commit"
    moon = rows[("moon_row_broken_k5", "moon")]
    assert moon["hazards"] == "incumbent-only"
    assert moon["requires_preexisting_authority"] == 1
    assert rows[("full_reachability_k5", "earth")]["hazards"] == ""


def test_rows_are_reconstructible_from_recorded_inputs():
    rows = _index(run())
    mars_row = rows[("mars_conjunction_k5", "mars")]
    assert (mars_row["r1"], mars_row["r2"]) == (0, 0)
    # The input reachable set is recorded, so the row can be recomputed.
    assert mars_row["reachable"] == "100;101;102"
    assert "tier 1" in mars_row["missing"]
    earth_row = rows[("mars_conjunction_k5", "earth")]
    assert (earth_row["r1"], earth_row["r2"]) == (1, 1)
    assert earth_row["r2_witness"] == "1;2;3;4;5"
