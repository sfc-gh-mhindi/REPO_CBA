{{ config(materialized='incremental', alias='_cba__app_csel4_sit_dataset_tmp__cse__com__cpo__bus__ncpr__clnt__appt__stus', incremental_strategy='insert_overwrite', tags=['XfmAppt_Stus']) }}

SELECT
	APPT_I,
	STUS_C,
	STRT_D,
	STRT_T,
	STRT_S,
	END_D,
	END_T,
	END_S,
	EMPL_I,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	RUN_STRM,
	keyChange 
FROM {{ ref('SrtApptI') }}