import re

COMPOSITE_RELEASE_ID_FIELD = "CompositeReleaseID"

PRODUCTS_INDEX = "*"

LAST_MODIFIED_FIELD = "LastModifiedTime"

PRODUCT_COUNTER = "ProductCounter"

TIMER_INDEX = "timer_status"

INPUT_PRODUCT_TYPE_TO_INDEX = {
    "HLS_L30": ["grq_*_l2_hls_l30", "grq_*_l2_hls_l30-*"],
    "HLS_S30": ["grq_*_l2_hls_s30", "grq_*_l2_hls_s30-*"],
    "L1_S1_SLC": ["grq_*_l1_s1_slc", "grq_*_l1_s1_slc-*"],
    "L2_RTC_S1": ["grq_*_l2_rtc_s1", "grq_*_l2_rtc_s1-*"]
}

PRODUCT_TYPE_TO_INDEX = {
    "L3_DSWX_HLS": ["grq_*_l3_dswx_hls", "grq_*_l3_dswx_hls-*"],
    "L2_CSLC_S1": ["grq_*_l2_cslc_s1", "grq_*_l2_cslc_s1-*"],
    "L2_RTC_S1": ["grq_*_l2_rtc_s1", "grq_*_l2_rtc_s1-*"],
    "L3_DSWX_S1": ["grq_*_l3_dswx_s1", "grq_*_l3_dswx_s1-*"],
    "L3_DISP_S1": ["grq_*_l3_disp_s1-*"],

    # Products in production that bach-api previously had no entry for. Keys use
    # the `dataset_type` value exactly as it appears in the datastore, because
    # that is what the UI displays and what TRANSFERABLE_PRODUCT_TYPES is
    # compared against. (The five keys above predate this and are upper-cased;
    # see the note at the end of this block.)
    "L3_DSWx_NI": ["grq_*_l3_dswx_ni", "grq_*_l3_dswx_ni-*"],
    "L3_DIST_S1": ["grq_*_l3_dist_s1", "grq_*_l3_dist_s1-*"],
    "L2_GCOV_NI": ["grq_*_l2_gcov_ni", "grq_*_l2_gcov_ni-*"],
    "L2_CSLC_S1_STATIC": ["grq_*_l2_cslc_s1_static", "grq_*_l2_cslc_s1_static-*"],
    "L2_RTC_S1_STATIC": ["grq_*_l2_rtc_s1_static", "grq_*_l2_rtc_s1_static-*"],
    "L2_CSLC_S1_COMPRESSED": ["grq_*_l2_cslc_s1_compressed", "grq_*_l2_cslc_s1_compressed-*"],
    "L4_TROPO": ["grq_*_l4_tropo", "grq_*_l4_tropo-*"]
}
"""Map of product types to their Elasticsearch indexes.

Index names are `grq_<version>_<type>-<YYYY.MM>`. The version segment is not
uniform across product types -- v1.0, v1.1, v0.1, 1, 1.0 and 2.0 are all in use
-- which is why every pattern wildcards it.

The trailing `-*` matters: the bare pattern matches nothing on its own, since
every product index carries a month suffix. Both forms are kept per type so an
index without a suffix would still be found.

The patterns are also deliberately anchored on a hyphen after the type, so
`grq_*_l2_rtc_s1-*` does NOT capture `grq_*_l2_rtc_s1_static-*`, and
`grq_*_l2_gcov_ni-*` does not capture `grq_*_l2_gcov_ni_batch-*`. Verified
against the live index list: the per-type sums match the `dataset_type`
aggregation exactly.

NOTE: the first five keys are upper-cased (`L3_DSWX_HLS`) while the datastore
reports `L3_DSWx_HLS`. That predates this change and is left alone here rather
than renamed, because the keys are used as display labels and in URL paths.
Worth reconciling separately."""

INPUT_PRODUCT_TYPE_TO_SDS_PRODUCT_TYPE = {
    "L2_HLS_L30": ["L3_DSWX_HLS"],
    "L2_HLS_S30": ["L3_DSWX_HLS"],
    "L1_S1_SLC": ["L2_CSLC_S1", "L2_RTC_S1"],
    "L2_RTC_S1": ["L3_DSWX_S1"],
    "L2_CSLC_S1": ["L3_DISP_S1"],
}
"""Map of input product types to their respective SDS product type."""

ACCOUNTABILITY_INDEXES = {
    "DOWNLINK": "pass_accountability_catalog",
    "OBSERVATION": "observation_accountability_catalog",
    "TRACK_FRAME": "track_frame_accountability_catalog"
}

INCOMING_SDP_PRODUCTS = {
    "HLS_L30": ["grq_*_l2_hls_l30", "grq_*_l2_hls_l30-*"],
    "HLS_S30": ["grq_*_l2_hls_s30", "grq_*_l2_hls_s30-*"],
    "L1_S1_SLC": ["grq_*_l1_s1_slc", "grq_*_l1_s1_slc-*"],
    "L2_RTC_S1": ["rtc_catalog-*"],
    "L2_CSLC_S1": ["cslc_catalog-*"]
}

# TODO chrisjrd: finalize.
INCOMING_ANCILLARY_FILES = {
}

GENERATED_PRODUCTS = {
    "DSWX_HLS": PRODUCT_TYPE_TO_INDEX["L3_DSWX_HLS"],
    "L2_CSLC_S1": PRODUCT_TYPE_TO_INDEX["L2_CSLC_S1"],
    "L2_RTC_S1": PRODUCT_TYPE_TO_INDEX["L2_RTC_S1"],
    "L3_DSWX_S1": PRODUCT_TYPE_TO_INDEX["L3_DSWX_S1"],
    "L3_DISP_S1": PRODUCT_TYPE_TO_INDEX["L3_DISP_S1"]
}

OUTGOING_PRODUCTS_TO_DAAC = {
    "DSWX_HLS": PRODUCT_TYPE_TO_INDEX["L3_DSWX_HLS"],
    "L2_CSLC_S1": PRODUCT_TYPE_TO_INDEX["L2_CSLC_S1"],
    "L2_RTC_S1": PRODUCT_TYPE_TO_INDEX["L2_RTC_S1"],
    "L3_DSWX_S1": PRODUCT_TYPE_TO_INDEX["L3_DSWX_S1"],
    "L3_DISP_S1": PRODUCT_TYPE_TO_INDEX["L3_DISP_S1"]
}

TRANSFERABLE_PRODUCT_TYPES = [
    "L3_DSWx_HLS",
    "L2_CSLC_S1",
    "L2_RTC_S1",
    "L3_DSWx_S1",
    "L3_DISP_S1"
]

RSLC_CHILDREN = []

MOZART_INDEXES = {"JOB_STATUS": "job_status-current", "TIMER": "timer_status"}


def granule_id_to_tile_id(granule_id: str) -> str:
    # example _id = HLS.L30.T22VEQ.2021248T143156.v2.0
    return re.findall(r"T\w{5}", granule_id)[0]


def granule_id_to_acquisition_ts(granule_id):
    # example _id = HLS.L30.T22VEQ.2021248T143156.v2.0
    return re.findall(r"\d{7}T\d{6}", granule_id)[0]


def granule_id_to_input_product_type(input_product_granule_id: str):
    # example _id = HLS.L30.T22VEQ.2021248T143156.v2.0
    if input_product_granule_id.startswith("HLS.L30"):
        return "L2_HLS_L30"
    elif input_product_granule_id.startswith("HLS.S30"):
        return "L2_HLS_S30"
    else:
        raise Exception(f"Unable to map {input_product_granule_id=} to an input product type")


def granule_id_to_sensor(granule_id):
    # example _id = HLS.L30.T22VEQ.2021248T143156.v2.0
    if ".L30" in granule_id:
        return "LANDSAT"
    elif ".S30" in granule_id:
        return "SENTINEL"
    else:
        raise Exception(f"Unable to map {granule_id=} to sensor")


def sds_product_id_to_sds_product_type(sds_product_id: str):
    # example _id = OPERA_L3_DSWx_HLS_T57NVH_20220117T000429Z_20220117T000429Z_S2A_30_v2.0
    if sds_product_id.startswith("OPERA_L3_DSWx_HLS"):
        return "L3_DSWX_HLS"
    # example _id = OPERA_L2_CSLC_S1A_IW_T64-135524-IW2_VV_20220501T015035Z_v0.1_20220501T015102Z
    if sds_product_id.startswith("OPERA_L2_CSLC_S1A"):
        return "L2_CSLC_S1"
    # example _id = OPERA_L2_RTC_S1A_IW_T64-135524-IW2_VV_20220501T015035Z_v0.1_20220501T015102Z
    if sds_product_id.startswith("OPERA_L2_RTC_S1A"):
        return "L2_RTC_S1"
    # example _id = OPERA_L2_RTC_S1B_IW_T64-135524-IW2_VV_20220501T015035Z_v0.1_20220501T015102Z
    if sds_product_id.startswith("OPERA_L2_RTC_S1B"):
        return "L2_RTC_S1"
    # example _id = OPERA_L3_DSWx_S1_
    if sds_product_id.startswith("OPERA_L3_DSWx-S1"):
        return "L3_DSWX_S1"
    # example _id = OPERA_L3_DISP_S1_
    if sds_product_id.startswith("OPERA_L3_DISP-S1"):
        return "L3_DISP_S1"
    else:
        raise Exception(f"Unable to map {sds_product_id=} to an SDS product type")


def sds_product_id_to_sensor(sds_product_id: str):
    # example _id = OPERA_L3_DSWx_HLS_T57NVH_20220117T000429Z_20220117T000429Z_S2A_30_v2.0
    # example _id = OPERA_L2_CSLC_S1A_IW_T64-135524-IW2_VV_20220501T015035Z_v0.1_20220501T015102Z
    # example _id = OPERA_L2_CSLC_S1B_IW_T64-135524-IW2_VV_20220501T015035Z_v0.1_20220501T015102Z
    if "_S1A" in sds_product_id:
        return "SENTINEL"
    if "_S1B" in sds_product_id:
        return "SENTINEL"
    elif "_S2A" in sds_product_id:
        return "SENTINEL"
    elif "_S2B" in sds_product_id:
        return "SENTINEL"
    elif "_L8" in sds_product_id:
        return "LANDSAT"
    elif "_L9" in sds_product_id:
        return "LANDSAT"
    else:
        raise Exception(f"Unable to map {sds_product_id=} to sensor")


def sds_product_id_to_tile_id(sds_product_id: str) -> str:
    return re.findall(r"T\w{5}", sds_product_id)[0]


def sds_product_id_to_acquisition_ts(sds_product_id: str) -> str:
    return re.findall(r"\d{8}T\d{6}", sds_product_id)[0]
