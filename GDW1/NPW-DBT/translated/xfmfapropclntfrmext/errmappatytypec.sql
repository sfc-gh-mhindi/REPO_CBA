{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_error_cse__coi__bus__prop__clnt__20061016__mappatytypec', incremental_strategy='insert_overwrite', tags=['XfmFaPropClntFrmExt']) }}

SELECT
	SRCE_KEY_I,
	CONV_M,
	CONV_MAP_RULE_M,
	TRSF_TABL_M,
	VALU_CHNG_BFOR_X,
	VALU_CHNG_AFTR_X,
	TRSF_X,
	TRSF_COLM_M 
FROM {{ ref('XfmBusinessRules__OutErrorMapPdctN') }}