{{
  config(
    post_hook=[
      'INSERT OVERWRITE INTO '~cvar("intermediate_db")~'.'~ cvar("files_schema") ~ '.'~ cvar("base_dir")~ '__lookupset__MAP_CSE_PACK_PDCT_PL_PL_PACK_CAT_ID__FS SELECT * FROM {{ this }}'
    ]
  )
}}

SELECT
	PL_PACKAGE_CAT_ID,
	PDCT_N 
FROM {{ ref('xfmconversions__ldmap_cse_pack_pdct_pllkp') }}