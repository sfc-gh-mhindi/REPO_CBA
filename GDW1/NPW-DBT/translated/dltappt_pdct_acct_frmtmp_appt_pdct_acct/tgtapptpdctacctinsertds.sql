{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__pdct__acct__i__cse__chl__bus__hlm__app__20100614', incremental_strategy='insert_overwrite', tags=['DltAPPT_PDCT_ACCT_FrmTMP_APPT_PDCT_ACCT']) }}

SELECT
	APPT_PDCT_I,
	ACCT_I,
	REL_TYPE_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptPdctAcctInsertDS') }}