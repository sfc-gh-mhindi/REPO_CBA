{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__pdct__chkl__i__cse__cpl__bus__app__prod__20101207', incremental_strategy='insert_overwrite', tags=['DltAPPT_PDCT_CHKLFrmTMP_APPT_PDCT_CHKL']) }}

SELECT
	APPT_PDCT_I,
	CHKL_ITEM_C,
	STUS_D,
	STUS_C,
	SRCE_SYST_C,
	CHKL_ITEM_X,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptPdctChklInsertDS') }}