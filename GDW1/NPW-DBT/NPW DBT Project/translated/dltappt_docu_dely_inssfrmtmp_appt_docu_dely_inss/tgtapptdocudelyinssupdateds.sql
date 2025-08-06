{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__docu__dely__inss__u__cse__chl__bus__app__20100914', incremental_strategy='insert_overwrite', tags=['DltAPPT_DOCU_DELY_INSSFrmTMP_APPT_DOCU_DELY_INSS']) }}

SELECT
	APPT_I,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptDocuDelyInssUpdateDS') }}