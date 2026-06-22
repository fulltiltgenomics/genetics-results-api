"""Unit tests for the has_credible_sets phenotype-search flag (config-level, no server).

The annotation phenotype search must offer every phenotype with credible sets — including
resources that have credible sets but no full summary stats (e.g. Open Targets) — while the
summary-stats phenotype view filters on has_summary_stats. has_credible_sets mirrors
has_summary_stats: a per-(resource, data_type) flag precomputed from the credible_sets config.
"""

from app.config.credible_sets import get_credible_set_resources_and_types
from app.config.summary_stats import get_available_resources_and_types


def test_returns_sorted_resource_data_type_pairs():
    pairs = get_credible_set_resources_and_types()
    assert isinstance(pairs, list)
    assert pairs == sorted(pairs)
    assert all(isinstance(p, tuple) and len(p) == 2 for p in pairs)
    # every configured credible-set resource shows up
    assert ("finngen", "gwas") in pairs


def test_open_targets_has_credible_sets_but_not_summary_stats():
    """The exact case the flag exists for: Open Targets has credible sets, no full sumstats."""
    cs = set(get_credible_set_resources_and_types())
    ss = set(get_available_resources_and_types())
    assert ("open_targets", "gwas") in cs
    assert ("open_targets", "gwas") not in ss


def test_credible_sets_and_summary_stats_pairs_are_not_identical():
    """If the two sets were identical the flag would be redundant; they genuinely differ."""
    cs = set(get_credible_set_resources_and_types())
    ss = set(get_available_resources_and_types())
    assert cs != ss
    # at least one (resource, data_type) has sumstats but no credible sets — those phenotypes
    # should be dropped from the annotation search even though they survive the sumstats view.
    assert ss - cs
