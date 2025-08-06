{{ config(materialized='incremental', alias='_cba__app_mme_dev_error_cse__chl__bus__app__prod__20081016__maptargcharc', incremental_strategy='insert_overwrite', tags=['XfmHL_APP_PRODFrmExt']) }}

SELECT
	SRCE_KEY_I,
	CONV_M,
	CONV_MAP_RULE_M,
	TRSF_TABL_M,
	VALU_CHNG_BFOR_X,
	VALU_CHNG_AFTR_X,
	TRSF_X,
	TRSF_COLM_M 
FROM {{ ref('XfmBusinessRules__OutErrorMapTargCharC') }}