{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__pdct__feat__u__cse__chl__bus__hlm__app__20150120', incremental_strategy='insert_overwrite', tags=['DltAPPT_PDCT_FEAT_FrmTMP_APPT_PDCT_FEAT_PEXAHM_ONEOFF']) }}

SELECT
	APPT_PDCT_I,
	FEAT_I,
	SRCE_SYST_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptPdctFeatUpdate') }}