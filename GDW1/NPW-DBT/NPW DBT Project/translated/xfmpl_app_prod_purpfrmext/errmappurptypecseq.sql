{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_error_cse__cpl__bus__app__prod__purp__20060616__mappurptypec', incremental_strategy='insert_overwrite', tags=['XfmPL_APP_PROD_PURPFrmExt']) }}

SELECT
	SRCE_KEY_I,
	CONV_M,
	CONV_MAP_RULE_M,
	TRSF_TABL_M,
	VALU_CHNG_BFOR_X,
	VALU_CHNG_AFTR_X,
	TRSF_X,
	TRSF_COLM_M 
FROM {{ ref('XfmBusinessRules__OutErrorMapPurpC') }}