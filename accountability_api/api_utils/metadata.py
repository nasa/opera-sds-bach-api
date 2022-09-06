import re

COMPOSITE_RELEASE_ID_FIELD = "CompositeReleaseID"

PRODUCTS_INDEX = "*"

LAST_MODIFIED_FIELD = "LastModifiedTime"

PRODUCT_COUNTER = "ProductCounter"

TIMER_INDEX = "timer_status"

INPUT_PRODUCT_TYPE_TO_INDEX = {
    "HLS_L30": "grq_*_l2_hls_l30",
    "HLS_S30": "grq_*_l2_hls_s30"
}

# TODO chrisjrd: finalize. Currently, the opera_state_config index does not behave like normal *-state-config indexes
#  and so cannot be added to this map.
STATE_CONFIG_INDEXES = {
    "L30_STATE_CONFIG": "grq_*_l2_hls_l30-state-config",
    "S30_STATE_CONFIG": "grq_*_l2_hls_s30-state-config"
}

PRODUCT_TYPE_TO_INDEX = {
    "L3_DSWX_HLS": "grq_*_l3_dswx_hls"
}
"""Map of product types to their Elasticsearch indexes."""

INPUT_PRODUCT_TYPE_TO_SDS_PRODUCT_TYPE = {
    "L2_HLS_L30": "L3_DSWX_HLS",
    "L2_HLS_S30": "L3_DSWX_HLS",
}
"""Map of input product types to their respective SDS product type."""

SDS_PRODUCT_TYPE_TO_INPUT_PRODUCT_TYPES = {
    "L3_DSWX_HLS": ["L2_HLS_L30", "L2_HLS_S30"]
}
"""Map of SDS product types to a list of input product types that they support. """

ACCOUNTABILITY_INDEXES = {
    "DOWNLINK": "pass_accountability_catalog",
    "OBSERVATION": "observation_accountability_catalog",
    "TRACK_FRAME": "track_frame_accountability_catalog"
}

INCOMING_SDP_PRODUCTS = {
    "HLS_L30": "grq_*_l2_hls_l30",
    "HLS_S30": "grq_*_l2_hls_s30"
}

# TODO chrisjrd: finalize.
INCOMING_ANCILLARY_FILES = {
}

GENERATED_PRODUCTS = {
    "DSWX_HLS": "grq_*_l3_dswx_hls"
}

OUTGOING_PRODUCTS_TO_DAAC = {
    "DSWX_HLS": "grq_*_l3_dswx_hls"
}

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
    else:
        raise Exception(f"Unable to map {sds_product_id=} to an SDS product type")


def sds_product_id_to_sensor(sds_product_id: str):
    # example _id = OPERA_L3_DSWx_HLS_T57NVH_20220117T000429Z_20220117T000429Z_S2A_30_v2.0
    if "_S2A" in sds_product_id:
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
