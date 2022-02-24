COMPOSITE_RELEASE_ID_FIELD = "CompositeReleaseID"

PRODUCTS_INDEX = "*"

LAST_MODIFIED_FIELD = "LastModifiedTime"

PRODUCT_COUNTER = "ProductCounter"

TIMER_INDEX = "timer_status"

ANCILLARY_INDEXES = {
    "ARP": "grq_*_arp",
    "COP": "grq_*_cop",
    "LRCLK-UTC": "grq_*_sclkscet",
    "FOE": "grq_*_foe",
    "FRP": "grq_*_frp",
    "MOE": "grq_*_moe",
    "NEN_L_RRST": "grq_*_nen_l_rrst",
    "NOE": "grq_*_noe",
    "NRP": "grq_*_nrp",
    "POE": "grq_*_poe",
    "PRP": "grq_*_prp",
    "ROP": "grq_*_rop",
    "RADAR_CFG": "grq_*_radar_cfg",
    "OROST": "grq_*_orost",
    "SROST": "grq_*_srost",
    "OFS": "grq_*_ofs",
    "STUF": "grq_*_stuf",
    "LDF": "grq_*_ldf"
}

STATE_CONFIG_INDEXES = {
    "LDF_STATE_CONFIG": "grq_*_ldf-state-config",
    "LDF_EXPIRED_STATE_CONFIG": "grq_*_ldf-expired-state-config",
    "DATATAKE_STATE_CONFIG": "grq_*_datatake-state-config",
    "DATATAKE_EXPIRED_STATE_CONFIG": "grq_*_datatake-expired-state-config",
    "DATATAKE_URGENT_RESPONSE_EXPIRED_STATE_CONFIG": "grq_*_datatake-urgent_response_expired-state-config",
    "DATATAKE_URGENT_RESPONSE_STATE_CONFIG": "grq_*_datatake-urgent_response_state-config",
    "TRACK_FRAME_STATE_CONFIG": "grq_*_track_frame-state-config",
}

PRODUCT_INDEXES = {
    "LDF_STATE_CONFIG": "grq_*_ldf-state-config",
    "LDF_EXPIRED_STATE_CONFIG": "grq_*_ldf-expired-state-config",
    "DATATAKE_STATE_CONFIG": "grq_*_datatake-state-config",
    "DATATAKE_EXPIRED_STATE_CONFIG": "grq_*_datatake-expired-state-config",
    "DATATAKE_URGENT_RESPONSE_EXPIRED_STATE_CONFIG": "grq_*_datatake-urgent_response_expired-state-config",
    "DATATAKE_URGENT_RESPONSE_STATE_CONFIG": "grq_*_datatake-urgent_response_state-config",
    "TRACK_FRAME_STATE_CONFIG": "grq_*_track_frame-state-config",
    "L0A_L_RRST_PP": "grq_*_l0a_l_rrst_pp",
    "L0A_L_RRST": "grq_*_l0a_l_rrst",
    "L0B_L_RRSD": "grq_*_l0b_l_rrsd",
    "L0B_L_CRSD": "grq_*_l0b_l_crsd",
    "L1_L_RSLC": "grq_*_l1_l_rslc",
    "L1_L_RIFG": "grq_*_l1_l_rifg",
    "L1_L_RUNW": "grq_*_l1_l_runw",
    "L2_L_GSLC": "grq_*_l2_l_gslc",
    "L2_L_GCOV": "grq_*_l2_l_gcov",
    "L2_L_GUNW": "grq_*_l2_l_gunw"
}

ACCOUNTABILITY_INDEXES = {
    "DOWNLINK": "pass_accountability_catalog",
    "OBSERVATION": "observation_accountability_catalog",
    "COP": "cop_catalog",
    "ROST": "rost_catalog"
}

INCOMING_NEN_PRODUCTS = {
    "NEN_L_RRST": "grq_*_nen_l_rrst",
    "LDF": "grq_*_ldf",
    "ARP": "grq_*_arp"
}

INCOMING_GDS_ANCILLARY_FILES = {
    "COP": "grq_*_cop",
    "TIURDROP": "grq_*_tiurdrop",
    "ROP": "grq_*_rop",
    "OROST": "grq_*_orost",
    "SROST": "grq_*_srost",
    "OFS": "grq_*_ofs",
    "MOE": "grq_*_moe",
    "POE": "grq_*_poe",
    "FOE": "grq_*_foe",
    "NOE": "grq_*_noe",
    "FRP": "grq_*_frp",
    "NRP": "grq_*_nrp",
    "PRP": "grq_*_prp",
    "LRCLK-UTC": "grq_*_sclkscet",
    "STUF": "grq_*_stuf"
}

GENERATED_PRODUCTS = {
    "LDF_STATE_CONFIG": "grq_*_ldf-state-config",
    "LDF_EXPIRED_STATE_CONFIG": "grq_*_ldf-expired-state-config",
    "DATATAKE_STATE_CONFIG": "grq_*_datatake-state-config",
    "DATATAKE_EXPIRED_STATE_CONFIG": "grq_*_datatake-expired-state-config",
    "DATATAKE_URGENT_RESPONSE_EXPIRED_STATE_CONFIG": "grq_*_datatake-urgent_response_expired-state-config",
    "DATATAKE_URGENT_RESPONSE_STATE_CONFIG": "grq_*_datatake-urgent_response_state-config",
    "TRACK_FRAME_STATE_CONFIG": "grq_*_track_frame-state-config",
    "L0A_L_RRST_PP": "grq_*_l0a_l_rrst_pp",
    "L0A_L_RRST": "grq_*_l0a_l_rrst",
    "L0B_L_RRSD": "grq_*_l0b_l_rrsd",
    "L0B_L_CRSD": "grq_*_l0b_l_crsd",
    "L1_L_RSLC": "grq_*_l1_l_rslc",
    "L1_L_RIFG": "grq_*_l1_l_rifg",
    "L1_L_RUNW": "grq_*_l1_l_runw",
    "L2_L_GSLC": "grq_*_l2_l_gslc",
    "L2_L_GCOV": "grq_*_l2_l_gcov",
    "L2_L_GUNW": "grq_*_l2_l_gunw"
}


MOZART_INDEXES = {"JOB_STATUS": "job_status-current", "TIMER": "timer_status"}
