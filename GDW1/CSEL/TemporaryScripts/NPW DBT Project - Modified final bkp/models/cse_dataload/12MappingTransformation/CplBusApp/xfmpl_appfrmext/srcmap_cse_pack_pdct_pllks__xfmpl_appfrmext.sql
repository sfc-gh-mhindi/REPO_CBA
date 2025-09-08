WITH 
srcmap_cse_pack_pdct_pllks AS (
SELECT 
    PL_PACKAGE_CAT_ID,
    PDCT_N
FROM {{ cvar("intermediate_db") }}.{{ cvar("files_schema") }}.{{ cvar("base_dir") }}__lookupset__MAP_CSE_PACK_PDCT_PL_PL_PACK_CAT_ID__FS
)

SELECT * FROM srcmap_cse_pack_pdct_pllks