{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__actv__u__cse__chl__bus__app__20100915', incremental_strategy='insert_overwrite', tags=['DltAPPT_ACTVFrmTMP_APPT_ACTV']) }}

SELECT
	APPT_I,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptActvUpdateDS') }}