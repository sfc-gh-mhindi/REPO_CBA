{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__pdct__feat__u__cse__chl__bus__app__20150115', incremental_strategy='insert_overwrite', tags=['DltAPPT_PDCT_FEAT_PEXAFrmTMP_APPT_FEAT_PEXA_Adhoc']) }}

SELECT
	APPT_PDCT_I,
	FEAT_I,
	SRCE_SYST_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptPdctFeatUpdate') }}