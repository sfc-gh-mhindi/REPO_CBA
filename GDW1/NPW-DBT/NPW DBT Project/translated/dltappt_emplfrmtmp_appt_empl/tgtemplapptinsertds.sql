{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_empl__appt__i__cse__com__bus__ccl__chl__com__app__20150727', incremental_strategy='insert_overwrite', tags=['DltAPPT_EMPLFrmTMP_APPT_EMPL']) }}

SELECT
	EMPL_I,
	APPT_I,
	EMPL_ROLE_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtEmplApptInsertDS') }}