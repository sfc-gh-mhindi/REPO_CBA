{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_appt__pdct__acct__u__cse__ccc__bus__app__prod__20060101', incremental_strategy='insert_overwrite', tags=['DltAPPT_PDCT_ACCTFrmTMP_APPT_PDCT_ACCT']) }}

SELECT
	APPT_PDCT_I,
	ACCT_I,
	REL_TYPE_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptPdctAcctUpdateDS') }}