import pytest

from pyodibel.datasets.mp_mf.overlap_util import (
    build_exact_subsets,
    pairwise_entity_overlaps,
    validate_overlaps,
)


class TestBuildExactSubsets:
    def test_subset_sizes(self):
        ids = list(range(1, 10_000))
        subsets = build_exact_subsets(ids, num_subsets=4, overlap_ratio=0.04, subset_size=250)
        assert len(subsets) == 4
        assert all(len(subset) == 250 for subset in subsets)

    def test_pairwise_overlap_ratio(self):
        ids = list(range(1, 10_000))
        overlap_ratio = 0.04
        subset_size = 250
        subsets = build_exact_subsets(
            ids,
            num_subsets=4,
            overlap_ratio=overlap_ratio,
            subset_size=subset_size,
        )
        overlaps = validate_overlaps(subsets)
        for ratio in overlaps.values():
            assert ratio == pytest.approx(overlap_ratio)

    def test_requires_integer_overlap_bucket(self):
        with pytest.raises(ValueError, match="integer"):
            build_exact_subsets(list(range(100)), num_subsets=4, overlap_ratio=0.03, subset_size=250)

    def test_requires_enough_ids(self):
        with pytest.raises(ValueError, match="Not enough IDs"):
            build_exact_subsets(list(range(10)), num_subsets=4, overlap_ratio=0.04, subset_size=250)


class TestPairwiseEntityOverlaps:
    def test_reports_count_and_percentages(self):
        overlaps = pairwise_entity_overlaps([
            {"a", "b", "c"},
            {"b", "c", "d"},
        ])
        assert overlaps["split_0-split_1"] == {
            "count": 2,
            "pct_left": pytest.approx(2 / 3),
            "pct_right": pytest.approx(2 / 3),
        }
