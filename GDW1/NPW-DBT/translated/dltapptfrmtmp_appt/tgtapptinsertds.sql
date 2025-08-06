{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__i__cse__com__bus__ccl__chl__com__app__20170306', incremental_strategy='insert_overwrite', tags=['DltAPPTFrmTMP_APPT']) }}

SELECT
	APPT_I,
	APPT_C,
	APPT_FORM_C,
	APPT_QLFY_C,
	STUS_TRAK_I,
	APPT_ORIG_C,
	APPT_N,
	SRCE_SYST_C,
	SRCE_SYST_APPT_I,
	APPT_CRAT_D,
	RATE_SEEK_F,
	PROS_KEY_EFFT_I,
	EROR_SEQN_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptInsertDS') }}