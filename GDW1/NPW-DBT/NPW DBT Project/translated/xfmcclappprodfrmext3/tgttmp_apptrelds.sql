{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_tmp__cse__ccl__bus__app__prod__appt__rel', incremental_strategy='insert_overwrite', tags=['XfmCclAppProdFrmExt3']) }}

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
FROM {{ ref('XfmBusinessRules__ApptRel1') }}