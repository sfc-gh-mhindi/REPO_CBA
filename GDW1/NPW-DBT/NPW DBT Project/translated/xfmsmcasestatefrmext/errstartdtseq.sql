{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_error_cse__com__bus__sm__case__state__20060616__maptargnumcc', incremental_strategy='insert_overwrite', tags=['XfmSmCaseStateFrmExt']) }}

SELECT
	SRCE_KEY_I,
	CONV_M,
	CONV_MAP_RULE_M,
	TRSF_TABL_M,
	VALU_CHNG_BFOR_X,
	VALU_CHNG_AFTR_X,
	TRSF_X,
	TRSF_COLM_M 
FROM {{ ref('XfmBusinessRules__OutErrorStartDate') }}