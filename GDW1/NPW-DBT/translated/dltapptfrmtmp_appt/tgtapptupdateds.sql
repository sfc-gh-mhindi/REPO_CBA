{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__u__cse__com__bus__ccl__chl__com__app__20170306', incremental_strategy='insert_overwrite', tags=['DltAPPTFrmTMP_APPT']) }}

SELECT
	APPT_I,
	APPT_C,
	APPT_FORM_C,
	STUS_TRAK_I,
	APPT_ORIG_C 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptUpdateDS') }}