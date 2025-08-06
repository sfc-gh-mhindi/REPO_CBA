{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__pdct__feat__u__cse__cpl__bus__fee__margin__20170306', incremental_strategy='insert_overwrite', tags=['DltAPPT_PDCT_PURPrmTMP_APPT_PDCT_PURP']) }}

SELECT
	APPT_PDCT_I,
	EFFT_D,
	SRCE_SYST_APPT_PDCT_PURP_I,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptPdctPurpUpdateDS') }}