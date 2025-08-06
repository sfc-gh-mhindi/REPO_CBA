{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_tmp__cse__ccl__bus__app__appt__ases__detl', incremental_strategy='insert_overwrite', tags=['XfmCclBusAppFrmExt']) }}

SELECT
	APPT_I,
	AMT_TYPE_C,
	EFFT_D,
	EXPY_D,
	CNCY_C,
	APPT_ASES_A,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	RUN_STRM 
FROM {{ ref('XfmBusinessRules__OutTmpCclApptAsesDetl') }}