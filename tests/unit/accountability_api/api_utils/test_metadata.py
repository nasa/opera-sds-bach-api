"""Guards on the product type -> index patterns.

The patterns in `metadata.py` are wildcards matched against real index names by
Elasticsearch/OpenSearch. Two properties are easy to break by eye and expensive
to notice in production, because a wrong pattern fails silently as a zero count
rather than as an error:

  1. every product type must actually match its indexes; and
  2. a type's pattern must not swallow a *neighbouring* type's indexes -- the
     `_static`, `_compressed` and `_batch` variants sit one character away from
     their parents.

`L2_GCOV_NI_BATCH` is deliberately absent from the registry: its documents are
batch manifests, so its indexes must match no product type at all.

The index names below were taken from the OPERA integration cluster on
2026-09-10 and cover every product-bearing index present at that time.
"""
from fnmatch import fnmatch

import pytest

from accountability_api.api_utils import metadata


# (index name, documents) as reported by _cat/indices on opera-int-fwd
LIVE_INDEXES = [
    "grq_v1.1_l3_dswx_hls-2026.08",
    "grq_v1.1_l2_cslc_s1-2026.08",
    "grq_v1.1_l2_cslc_s1-2026.09",
    "grq_v1.0_l2_rtc_s1-2026.08",
    "grq_v1.0_l2_rtc_s1-2026.09",
    "grq_v1.0_l3_dswx_s1-2026.08",
    "grq_v1.0_l3_dswx_s1-2026.09",
    "grq_v1.0_l3_disp_s1-2026.08",
    "grq_v0.1_l3_dswx_ni-2026.08",
    "grq_v0.1_l3_dswx_ni-2026.09",
    "grq_v1.0_l3_dist_s1-2026.08",
    "grq_v1.0_l3_dist_s1-2026.09",
    "grq_1_l2_gcov_ni-2026.08",
    "grq_1_l2_gcov_ni-2026.09",
    # GCOV batch manifests: grouping records naming the granules a downstream
    # job should process together, not products. Kept here on purpose -- they
    # are what `grq_*_l2_gcov_ni-*` must not swallow.
    "grq_1_l2_gcov_ni_batch-2026.08",
    "grq_1_l2_gcov_ni_batch-2026.09",
    "grq_v1.0_l2_cslc_s1_static-2026.08",
    "grq_v1.0_l2_rtc_s1_static-2026.08",
    "grq_1_l2_cslc_s1_compressed-2026.08",
    "grq_v1.0_l4_tropo-2026.08",
    "grq_1_l1_s1_slc-2026.08",
    "grq_1_l1_s1_slc-2026.09",
    "grq_v2.0_l2_hls_l30-2026.08",
    "grq_v2.0_l2_hls_s30-2026.08",
    # state-config and bookkeeping indexes -- must never match a product type
    "grq_1.0_dist_s1-state-config",
    "grq_1.0_dist_s1-fwd-state-config",
    "grq_1_dswx_ni-state-config-2026.08",
    "grq_1_dswx_ni-expired-state-config-2026.09",
    "grq_1_cslc_s1-cycle-state-config-2026.08",
    "grq_2.0_area_of_interest",
    "grq_v3.3.2_triaged_job",
]

EXPECTED = {
    "L3_DSWX_HLS": {"grq_v1.1_l3_dswx_hls-2026.08"},
    "L2_CSLC_S1": {"grq_v1.1_l2_cslc_s1-2026.08", "grq_v1.1_l2_cslc_s1-2026.09"},
    "L2_RTC_S1": {"grq_v1.0_l2_rtc_s1-2026.08", "grq_v1.0_l2_rtc_s1-2026.09"},
    "L3_DSWX_S1": {"grq_v1.0_l3_dswx_s1-2026.08", "grq_v1.0_l3_dswx_s1-2026.09"},
    "L3_DISP_S1": {"grq_v1.0_l3_disp_s1-2026.08"},
    "L3_DSWx_NI": {"grq_v0.1_l3_dswx_ni-2026.08", "grq_v0.1_l3_dswx_ni-2026.09"},
    "L3_DIST_S1": {"grq_v1.0_l3_dist_s1-2026.08", "grq_v1.0_l3_dist_s1-2026.09"},
    "L2_GCOV_NI": {"grq_1_l2_gcov_ni-2026.08", "grq_1_l2_gcov_ni-2026.09"},
    "L2_CSLC_S1_STATIC": {"grq_v1.0_l2_cslc_s1_static-2026.08"},
    "L2_RTC_S1_STATIC": {"grq_v1.0_l2_rtc_s1_static-2026.08"},
    "L2_CSLC_S1_COMPRESSED": {"grq_1_l2_cslc_s1_compressed-2026.08"},
    "L4_TROPO": {"grq_v1.0_l4_tropo-2026.08"},
}


def _matches(patterns):
    return {i for i in LIVE_INDEXES for p in patterns if fnmatch(i, p)}


@pytest.mark.parametrize("product_type", sorted(EXPECTED))
def test_product_type_matches_exactly_its_own_indexes(product_type):
    """Each type resolves to its indexes and to nothing else.

    This is the assertion that catches `grq_*_l2_rtc_s1-*` accidentally
    swallowing `grq_*_l2_rtc_s1_static-*`.
    """
    assert _matches(metadata.PRODUCT_TYPE_TO_INDEX[product_type]) == EXPECTED[product_type]


def test_every_configured_product_type_is_covered_by_this_test():
    """Fail if someone adds a product type without extending these fixtures."""
    assert set(metadata.PRODUCT_TYPE_TO_INDEX) == set(EXPECTED)


def test_no_product_type_matches_a_state_config_index():
    housekeeping = {i for i in LIVE_INDEXES if "state-config" in i or "triaged" in i or "area_of_interest" in i}
    for product_type, patterns in metadata.PRODUCT_TYPE_TO_INDEX.items():
        assert not (_matches(patterns) & housekeeping), f"{product_type} matches a non-product index"


def test_report_sets_cover_every_product_type():
    """The reports read from their own collections; none may silently lag behind."""
    for name in ("GENERATED_PRODUCTS", "OUTGOING_PRODUCTS_TO_DAAC"):
        collection = getattr(metadata, name)
        missing = set(metadata.PRODUCT_TYPE_TO_INDEX) - set(collection)
        # DSWX_HLS is keyed differently in these two collections, for historical reasons
        assert missing <= {"L3_DSWX_HLS"}, f"{name} is missing {missing}"


def test_input_product_chains_point_at_real_product_types():
    known = set(metadata.PRODUCT_TYPE_TO_INDEX)
    for input_type, outputs in metadata.INPUT_PRODUCT_TYPE_TO_SDS_PRODUCT_TYPE.items():
        for output in outputs:
            assert output in known, f"{input_type} -> {output}, which is not a known product type"


def test_gcov_batch_manifests_are_not_treated_as_products():
    """The `_batch` indexes hold grouping records, not products.

    Each document carries a `count` and a `granules` list naming the GCOV
    granules a downstream job should process together, so counting them
    alongside the granules themselves would double-report the same data.
    They must therefore match no product type -- including their own parent,
    `L2_GCOV_NI`, whose pattern sits one character away.
    """
    batch = {i for i in LIVE_INDEXES if "_gcov_ni_batch" in i}
    assert batch, "fixture no longer covers the GCOV batch indexes"
    for product_type, patterns in metadata.PRODUCT_TYPE_TO_INDEX.items():
        assert not (_matches(patterns) & batch), f"{product_type} matches a GCOV batch index"
