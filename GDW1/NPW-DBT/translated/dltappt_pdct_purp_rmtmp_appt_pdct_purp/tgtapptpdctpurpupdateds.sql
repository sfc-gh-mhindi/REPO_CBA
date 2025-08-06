{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__pdct__purp__u__cse__chl__bus__hlm__app__20100614', incremental_strategy='insert_overwrite', tags=['DltAPPT_PDCT_PURP_rmTMP_APPT_PDCT_PURP']) }}

SELECT
	APPT_PDCT_I,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptPdctPurpUpdateDS') }}