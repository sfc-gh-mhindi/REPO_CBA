{{ config(materialized='incremental', alias='_cba__app_csel4_sit_dataset_appt__qstn__u__cse__com__cpo__bus__ncpr__clnt__20110118', incremental_strategy='insert_overwrite', tags=['DltAppt_QstnFrmTMP']) }}

SELECT
	APPT_I,
	QSTN_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('GetGDWFields') }}