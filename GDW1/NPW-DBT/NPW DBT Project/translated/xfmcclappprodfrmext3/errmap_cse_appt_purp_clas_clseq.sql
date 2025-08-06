{{ config(materialized='incremental', alias='_cba__app_csel4_dev_error_cse__ccl__bus__app__prod__20091227__map__cse__appt__purp__clas__cl', incremental_strategy='insert_overwrite', tags=['XfmCclAppProdFrmExt3']) }}

SELECT
	SRCE_KEY_I,
	CONV_M,
	CONV_MAP_RULE_M,
	TRSF_TABL_M,
	VALU_CHNG_BFOR_X,
	VALU_CHNG_AFTR_X,
	TRSF_X,
	TRSF_COLM_M 
FROM {{ ref('XfmBusinessRules__OutErrorMAP_CSE_APPT_PURP_CLAS_CLSeq') }}