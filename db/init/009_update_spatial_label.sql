UPDATE dim_spatial
SET spatial_label = TRIM(
    CONCAT_WS(
        ' ',
        NULLIF(sido_name, ''),
        NULLIF(sig_name, ''),
        NULLIF(emd_name, '')
    )
)
WHERE
    (sido_name IS NOT NULL OR sig_name IS NOT NULL OR emd_name IS NOT NULL);
