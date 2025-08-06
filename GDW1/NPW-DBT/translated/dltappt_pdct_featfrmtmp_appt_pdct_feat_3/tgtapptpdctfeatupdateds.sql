{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_appt__pdct__feat__3__u__cse__chl__bus__feat__attr__20061016', incremental_strategy='insert_overwrite', tags=['DltAPPT_PDCT_FEATFrmTMP_APPT_PDCT_FEAT_3']) }}

SELECT
	APPT_PDCT_I,
	SRCE_SYST_APPT_FEAT_I,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptPdctFeatUpdateDS') }}