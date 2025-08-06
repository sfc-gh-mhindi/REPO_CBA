{{ config(materialized='incremental', alias='_cba__app_csel4_dev_error_cse__xs__chl__cpl__bus__app__ans__20101229__qstnrspcode', incremental_strategy='insert_overwrite', tags=['Ext_APP_ANS']) }}

SELECT
	SRCE_KEY_I,
	CONV_M,
	CONV_MAP_RULE_M,
	TRSF_TABL_M,
	VALU_CHNG_BFOR_X,
	VALU_CHNG_AFTR_X,
	TRSF_X,
	TRSF_COLM_M 
FROM {{ ref('Funnel_258') }}