{{
  config(
    post_hook=[
      'INSERT INTO '~ cvar("intermediate_db") ~ '.'~ cvar("files_schema") ~ '.'~ cvar("pinprocess") ~ '__' ~ cvar("prun_strm_c") ~ '_' ~ cvar("pcerr_table") ~ '_' ~ cvar("pctable_name") ~ '_' ~ cvar("prun_strm_pros_d") ~ '__TXT SELECT * FROM {{ this }}'
    ]
  )
}}

SELECT
	SRCE_KEY_I,
	CONV_M,
	CONV_MAP_RULE_M,
	TRSF_TABL_M,
	SRCE_EFFT_D,
	VALU_CHNG_BFOR_X,
	VALU_CHNG_AFTR_X,
	TRSF_X,
	TRSF_COLM_M,
	EROR_SEQN_I,
	SRCE_FILE_M,
	PROS_KEY_EFFT_I,
	TRSF_KEY_I 
FROM {{ ref('funnel__xfmplanbalnsegmmstrfrombcfinsg') }}