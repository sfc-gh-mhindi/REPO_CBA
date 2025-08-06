{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__pdct__amt__u__cse__com__cpo__bus__ncpr__clnt__20101015', incremental_strategy='insert_overwrite', tags=['DltAppt_Pdct_AmtFrmTMP']) }}

SELECT
	APPT_PDCT_I,
	AMT_TYPE_C,
	SRCE_SYST_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('GetGDWFields') }}