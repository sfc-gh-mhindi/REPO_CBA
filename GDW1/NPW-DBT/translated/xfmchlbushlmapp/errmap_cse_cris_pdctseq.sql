{{ config(materialized='incremental', alias='_cba__app_csel4_dev_error_cse__chl__bus__hlmapp__20100614__map__cse__cris__pdct', incremental_strategy='insert_overwrite', tags=['XfmChlBusHlmApp']) }}

SELECT
	SRCE_KEY_I,
	CONV_M,
	CONV_MAP_RULE_M,
	TRSF_TABL_M,
	VALU_CHNG_BFOR_X,
	VALU_CHNG_AFTR_X,
	TRSF_X,
	TRSF_COLM_M 
FROM {{ ref('XfmBusinessRules__OutErrorMAP_CSE_CRIS_PDCTSeq') }}