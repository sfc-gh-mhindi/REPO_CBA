{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_tmp__cse__ccl__bus__hl__app__appt__rel', incremental_strategy='insert_overwrite', tags=['XfmCCL_HL_APPFrmExt']) }}

SELECT
	APPT_I,
	RELD_APPT_I,
	REL_TYPE_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	RUN_STRM 
FROM {{ ref('XfmBusinessRules') }}