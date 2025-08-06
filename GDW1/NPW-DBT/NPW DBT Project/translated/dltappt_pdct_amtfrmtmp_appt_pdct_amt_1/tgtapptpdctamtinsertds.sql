{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_appt__pdct__amt__1__i__cse__chl__bus__app__prod__purp__20060101', incremental_strategy='insert_overwrite', tags=['DltAPPT_PDCT_AMTFrmTMP_APPT_PDCT_AMT_1']) }}

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