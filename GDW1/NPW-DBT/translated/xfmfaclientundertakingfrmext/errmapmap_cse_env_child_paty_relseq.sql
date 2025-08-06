{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_error_cse__coi__bus__envi__evnt__20060101__map__cse__env__child__paty__rel', incremental_strategy='insert_overwrite', tags=['XfmFaClientUndertakingFrmExt']) }}

SELECT
	SRCE_KEY_I,
	CONV_M,
	CONV_MAP_RULE_M,
	TRSF_TABL_M,
	VALU_CHNG_BFOR_X,
	VALU_CHNG_AFTR_X,
	TRSF_X,
	TRSF_COLM_M 
FROM {{ ref('XfmBusinessRules__OutMAP_CSE_ENV_CHILD_PATY_RELMapSeq') }}