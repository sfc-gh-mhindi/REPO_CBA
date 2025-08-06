{{
  config(
    post_hook=[
      "INSERT overwrite INTO " ~ cvar("files_schema") ~ ".cba_app__CSEL4__CSEL4DEV__error__CSE_CPL_BUS_APP_YYYYMMDD_MapPdctN__ERR SELECT * FROM {{ this }}"
    ]
  )
}}

WITH XfmBusinessRules__OutErrorMapPdctN AS (
  SELECT
    PL_APP_ID as SRCE_KEY_I,
    'PL_PACKAGE_CAT_ID' as CONV_M,
    'LOOKUP' as CONV_MAP_RULE_M,
    'MAP_CSE_PACK_PDCT_PL' as TRSF_TABL_M,
    PDCT_N as VALU_CHNG_BFOR_X,
    svPdctN as VALU_CHNG_AFTR_X,
    'Job Name = XfmPL_APPFrmExt' as TRSF_X,
    'PDCT_N' as TRSF_COLM_M
  From {{ ref('xfmbusinessrules__xfmpl_appfrmext') }}
  where svErrorCode = 'RPR2104'  and svLoadApptPdct = 'Y'
)

SELECT * FROM XfmBusinessRules__OutErrorMapPdctN