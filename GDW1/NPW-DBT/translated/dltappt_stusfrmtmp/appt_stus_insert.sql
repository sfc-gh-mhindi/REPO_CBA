{{ config(materialized='incremental', alias='_cba__app_csel4_sit_dataset_appt__stus__i__cse__com__cpo__bus__ncpr__clnt__20110112', incremental_strategy='insert_overwrite', tags=['DltAppt_StusFrmTMP']) }}

SELECT
	APPT_I,
	STUS_C,
	STRT_S,
	STRT_D,
	STRT_T,
	END_D,
	END_T,
	END_S,
	EMPL_I,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I 
FROM {{ ref('Update__ApptStus_Ins') }}