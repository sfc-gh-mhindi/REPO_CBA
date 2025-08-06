{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_tmp__cse__cpl__bus__app__appt__dept', incremental_strategy='insert_overwrite', tags=['XfmPL_APPFrmExt']) }}

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
FROM {{ ref('XfmBusinessRules__OutTmpApptDeptDS') }}