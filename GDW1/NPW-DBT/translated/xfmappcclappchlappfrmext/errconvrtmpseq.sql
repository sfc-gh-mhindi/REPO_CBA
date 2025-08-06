{{ config(materialized='incremental', alias='_cba__app_csel4_dev_error_cse__com__bus__ccl__chl__com__app__20120427__cse__receiveddate__app', incremental_strategy='insert_overwrite', tags=['XfmAppCclAppChlAppFrmExt']) }}

SELECT
	SRCE_KEY_I,
	CONV_M,
	CONV_MAP_RULE_M,
	TRSF_TABL_M,
	VALU_CHNG_BFOR_X,
	VALU_CHNG_AFTR_X,
	TRSF_X,
	TRSF_COLM_M 
FROM {{ ref('XfmBusinessRules__OutErrorRecvDate') }}