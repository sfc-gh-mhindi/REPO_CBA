{{ config(materialized='incremental', alias='_cba__app_hlt_dev_error_cse__chl__bus__tu__app__fund__det__20071105__mapcmpeid', incremental_strategy='insert_overwrite', tags=['XfmTu_ApptPdctFnddInssFrmExt']) }}

SELECT
	SRCE_KEY_I,
	CONV_M,
	CONV_MAP_RULE_M,
	TRSF_TABL_M,
	VALU_CHNG_BFOR_X,
	VALU_CHNG_AFTR_X,
	TRSF_X,
	TRSF_COLM_M 
FROM {{ ref('XfmBusinessRules__OutErrorMapCmpeID') }}