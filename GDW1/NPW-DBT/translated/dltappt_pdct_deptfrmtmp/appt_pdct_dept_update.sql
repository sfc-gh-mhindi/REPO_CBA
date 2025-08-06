{{ config(materialized='incremental', alias='_cba__app_csel4_sit_dataset_appt__pdct__dept__u__cse__com__cpo__bus__ncpr__clnt__20101015', incremental_strategy='insert_overwrite', tags=['DltAppt_Pdct_DeptFrmTMP']) }}

SELECT
	APPT_PDCT_I,
	DEPT_ROLE_C,
	SRCE_SYST_C,
	EXPY_D,
	PROS_KEY_EXPY_I,
	EFFT_D 
FROM {{ ref('GetGDWFields') }}