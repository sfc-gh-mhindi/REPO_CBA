{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__pdct__purp__i__cse__chl__bus__hlm__app__20100614', incremental_strategy='insert_overwrite', tags=['DltAPPT_PDCT_PURP_rmTMP_APPT_PDCT_PURP']) }}

SELECT
	APPT_PDCT_I,
	EFFT_D,
	SRCE_SYST_APPT_PDCT_PURP_I,
	PURP_TYPE_C,
	PURP_CLAS_C,
	SRCE_SYST_C,
	PURP_A,
	CNCY_C,
	MAIN_PURP_F,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptPdctPurpInsertDS') }}