{{ config(materialized='incremental', alias='_cba__app_hlt_sit_error_cse__clp__bus__appt__rel__20080310__maploanapp', incremental_strategy='insert_overwrite', tags=['XfmCPL_APP_PROD_Appt_RelExt']) }}

SELECT
	SRCE_KEY_I,
	CONV_M,
	CONV_MAP_RULE_M,
	TRSF_TABL_M,
	VALU_CHNG_BFOR_X,
	VALU_CHNG_AFTR_X,
	TRSF_X,
	TRSF_COLM_M 
FROM {{ ref('XfmBusinessRules__OutErrorMapLoanApp') }}