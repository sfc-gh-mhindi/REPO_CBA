{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__pdct__amt__2__i__cse__ccc__bus__app__prod__20170727', incremental_strategy='insert_overwrite', tags=['DltAPPT_PDCT_AMTFrmTMP_APPT_PDCT_AMT_2']) }}

SELECT
	APPT_PDCT_I,
	AMT_TYPE_C,
	EFFT_D,
	EXPY_D,
	CNCY_C,
	APPT_PDCT_A,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptPdctAmtInsertDS') }}