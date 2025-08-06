{{ config(materialized='incremental', alias='_cba__app_csel4_csel4__prd_error_cse__clp__bus__appt__pdct__feat__20080406__map__apf', incremental_strategy='insert_overwrite', tags=['XfmCPL_APP_PROD_Appt_Pdct_FeatExt']) }}

SELECT
	SRCE_KEY_I,
	CONV_M,
	CONV_MAP_RULE_M,
	TRSF_TABL_M,
	VALU_CHNG_BFOR_X,
	VALU_CHNG_AFTR_X,
	TRSF_X,
	TRSF_COLM_M 
FROM {{ ref('XfmBusinessRules__OutErrorMapCamp') }}