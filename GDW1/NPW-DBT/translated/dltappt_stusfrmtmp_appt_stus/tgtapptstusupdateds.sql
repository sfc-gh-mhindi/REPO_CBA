{{ config(materialized='incremental', alias='_cba__app_mme_dev_dataset_appt__stus__u__cse__com__bus__sm__case__state__20090123', incremental_strategy='insert_overwrite', tags=['DltAPPT_STUSFrmTMP_APPT_STUS']) }}

SELECT
	APPT_I,
	STUS_C,
	STRT_S,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptStusUpdateDS') }}