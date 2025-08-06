{{ config(materialized='incremental', alias='_cba__app_csel4_csel4__prd_error_cse__com__bus__ccl__chl__com__app__20080319__mapcnty', incremental_strategy='insert_overwrite', tags=['XfmChlBusPrtyAdrs']) }}

SELECT
	SRCE_KEY_I,
	CONV_M,
	CONV_MAP_RULE_M,
	TRSF_TABL_M,
	VALU_CHNG_BFOR_X,
	VALU_CHNG_AFTR_X,
	TRSF_X,
	TRSF_COLM_M 
FROM {{ ref('XfmBusinessRules__OutErrorMapCnty') }}