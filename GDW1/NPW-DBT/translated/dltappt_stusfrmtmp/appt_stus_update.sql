{{ config(materialized='incremental', alias='_cba__app_csel4_sit_dataset_appt__stus__u__cse__com__cpo__bus__ncpr__clnt__20110112', incremental_strategy='insert_overwrite', tags=['DltAppt_StusFrmTMP']) }}

SELECT
	APPT_I,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('GetGDWFields') }}