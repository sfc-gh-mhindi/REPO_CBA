{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_tmp__cse__com__cpo__bus__ncpr__clnt__appt__dept', incremental_strategy='insert_overwrite', tags=['XfmAppt_Dept']) }}

SELECT
	APPT_I,
	DEPT_I,
	DEPT_ROLE_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	RUN_STRM 
FROM {{ ref('Xfm__Xfm_to_Tgt') }}