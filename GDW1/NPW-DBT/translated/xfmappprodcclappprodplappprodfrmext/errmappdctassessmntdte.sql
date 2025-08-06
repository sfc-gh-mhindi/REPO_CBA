{{ config(materialized='incremental', alias='_cba__app_pj__gisswrteam_csel4_dev_error_cse__com__bus__app__prod__ccl__pl__app__prod__20150515__asessmentdate', incremental_strategy='insert_overwrite', tags=['XfmAppProdCclAppProdPlAppProdFrmExt']) }}

SELECT
	SRCE_KEY_I,
	CONV_M,
	CONV_MAP_RULE_M,
	TRSF_TABL_M,
	VALU_CHNG_BFOR_X,
	VALU_CHNG_AFTR_X,
	TRSF_X,
	TRSF_COLM_M 
FROM {{ ref('XfmBusinessRules__OutErrorMapPdctNAssessmntDate') }}