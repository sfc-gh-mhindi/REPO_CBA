{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_cse__appt__cpgn__u__cse__chl__bus__app__20060616', incremental_strategy='insert_overwrite', tags=['DltCSE_APPT_CPGNFrmTMP_CSE_APPT_CPGN']) }}

SELECT
	APPT_I,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('xf_delta__ln_updates') }}