{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__appt__empl', incremental_strategy='insert_overwrite', tags=['XfmAppCclAppChlAppFrmExt']) }}

SELECT
	APPT_I,
	EMPL_I,
	EMPL_ROLE_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	RUN_STRM 
FROM {{ ref('FnApptEmpl') }}