{{ config(materialized='incremental', alias='_cba__app_csel4_dev_error_cse__com__bus__ccl__chl__com__appmapcseapptcodehm__20120427', incremental_strategy='insert_overwrite', tags=['XfmAppCclAppChlAppFrmExt']) }}

SELECT
	SRCE_KEY_I,
	CONV_M,
	CONV_MAP_RULE_M,
	TRSF_TABL_M,
	SRCE_EFFT_D,
	VALU_CHNG_BFOR_X,
	VALU_CHNG_AFTR_X,
	TRSF_X,
	TRSF_COLM_M,
	EROR_SEQN_I,
	SRCE_FILE_M,
	PROS_KEY_EFFT_I,
	TRSF_KEY_I 
FROM {{ ref('XfmBusinessRules__OutErrorMAP_CSE_APPT_CODE_HMSeq') }}