{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__appt__dept', incremental_strategy='insert_overwrite', tags=['XfmAppCclAppChlAppFrmExt']) }}

SELECT
	APPT_I,
	DEPT_ROLE_C,
	EFFT_D,
	DEPT_I,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	RUN_STRM 
FROM {{ ref('XfmBusinessRules__OutApptDeptDS') }}