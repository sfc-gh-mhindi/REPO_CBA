{{ config(materialized='incremental', alias='_cba__app_hlt_dev_error_cse__chl__bus__tu__app__cond__20071019__mapapptqlf', incremental_strategy='insert_overwrite', tags=['XfmTu_App_Cnd_XFERFrmExt']) }}

SELECT
	SRCE_KEY_I,
	CONV_M,
	CONV_MAP_RULE_M,
	TRSF_TABL_M,
	VALU_CHNG_BFOR_X,
	VALU_CHNG_AFTR_X,
	TRSF_X,
	TRSF_COLM_M 
FROM {{ ref('OutRejApptQfy__qlf_map_err') }}