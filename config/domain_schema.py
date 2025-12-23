DOMAIN_INPUT_SCHEMA = {
    "sales": {
        "time_column": "STD_YM",
        "spatial_candidates": ["SGG_NAME", "LDONG_NAME", "LDONG", "SGG_CODE"],
        "category_column": "SCLS_NM",
        "category_fields": {
            "large": "LCLS_NM",
            "medium": "MCLS_NM",
            "small": "SCLS_NM",
        },
        "sales_prefixes": ["MAN_SALE_AMT_", "WMAN_SALE_AMT_"],
        "count_prefixes": ["MAN_APV_CNT_", "WMAN_APV_CNT_"],
    },
    "telco": {
        "time_column": "CRTR_YM",
        "day_column": "DAY_CNT",
        "spatial_column": "VST_SGG_CD",
        "male_prefix": "ML_VST_PPLTN_",
        "female_prefix": "FM_VST_PPLTN_",
    },
    "telco_grid": {
        "date_column": "CRTR_YMD",
        "month_column": "CRTR_YM",
        "spatial_column": "SM_LC_CD",
        "coord_x_column": "COORD_X",
        "coord_y_column": "COORD_Y",
        "male_prefixes": ["VST_ML_PPLTN_CNT_"],
        "female_prefixes": ["VST_FM_PPLTN_CNT_"],
    },
}
