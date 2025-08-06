{{ config(materialized='incremental', alias='_cba__app_hlt_sit_dataset_paty__appt__u__cse__ccl__bus__app__client__20080915', incremental_strategy='insert_overwrite', tags=['DltAPPT_PATYFrmTMP_APPT_PATY']) }}

SELECT
	APPT_I,
	PATY_I,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtPatyApptUpdateDS') }}