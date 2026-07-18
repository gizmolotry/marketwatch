from marketleak.detectors import benjamini_hochberg, causal_tail_diagnostic


def test_bh_fdr_is_monotone_by_rank() -> None:
    adjusted = benjamini_hochberg([0.04, 0.001, 0.02])
    ranked = sorted(zip([0.04, 0.001, 0.02], adjusted), key=lambda item: item[1][1])
    assert [item[1][1] for item in ranked] == [1, 2, 3]
    assert [item[1][0] for item in ranked] == sorted(item[1][0] for item in ranked)


def test_zero_mad_empirical_tail_requires_adequate_n() -> None:
    assert causal_tail_diagnostic(
        1.0,
        [0.0] * 20,
        min_observations=10,
        empirical_tail_min_observations=100,
    ) is None
    diagnostic = causal_tail_diagnostic(
        1.0,
        [0.0] * 100,
        min_observations=10,
        empirical_tail_min_observations=100,
    )
    assert diagnostic is not None
    assert diagnostic.method == "smoothed_empirical_tail"
    assert diagnostic.baseline_mad == 0
    assert diagnostic.p_value == 1 / 101
