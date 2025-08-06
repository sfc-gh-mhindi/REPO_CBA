{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__pdct__u__cse__com__bus__app__prod__ccl__pl__app__prod__20170306', incremental_strategy='insert_overwrite', tags=['DltAPPT_PDCTFrmTMP_APPT_PDCT']) }}

SELECT
	APPT_PDCT_I,
	APPT_I,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptPdctUpdateDS') }}