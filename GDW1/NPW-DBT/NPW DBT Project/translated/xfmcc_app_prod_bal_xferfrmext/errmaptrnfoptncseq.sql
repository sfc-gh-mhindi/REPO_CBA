{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_error_cse__ccc__bus__app__prod__bal__xfer__20060616__maptrnfoptnc', incremental_strategy='insert_overwrite', tags=['XfmCC_APP_PROD_BAL_XFERFrmExt']) }}

SELECT
	SRCE_KEY_I,
	CONV_M,
	CONV_MAP_RULE_M,
	TRSF_TABL_M,
	VALU_CHNG_BFOR_X,
	VALU_CHNG_AFTR_X,
	TRSF_X,
	TRSF_COLM_M 
FROM {{ ref('XfmBusinessRules__OutErrorMapTrnfOptnC') }}