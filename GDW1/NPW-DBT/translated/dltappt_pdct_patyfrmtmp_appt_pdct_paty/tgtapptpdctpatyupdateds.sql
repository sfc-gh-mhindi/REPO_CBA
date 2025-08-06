{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_appt__pdct__paty__u__cse__cpl__bus__fee__margin__20060101', incremental_strategy='insert_overwrite', tags=['DltAPPT_PDCT_PATYFrmTMP_APPT_PDCT_PATY']) }}

SELECT
	APPT_PDCT_I,
	PATY_I,
	PATY_ROLE_C,
	EFFT_D,
	PROS_KEY_EXPY_I,
	EXPY_D 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptPdctPatyUpdateDS') }}