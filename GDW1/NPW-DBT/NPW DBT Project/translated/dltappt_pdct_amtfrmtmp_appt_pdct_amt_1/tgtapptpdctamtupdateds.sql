{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_appt__pdct__amt__1__u__cse__chl__bus__app__prod__purp__20060101', incremental_strategy='insert_overwrite', tags=['DltAPPT_PDCT_AMTFrmTMP_APPT_PDCT_AMT_1']) }}

SELECT
	APPT_PDCT_I,
	AMT_TYPE_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptPdctAmtUpdateDS') }}