{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_cse__appt__pdct__ofi__setl__u__cse__chl__bus__hlm__app__20150117', incremental_strategy='insert_overwrite', tags=['DltAPPT_PDCT_OFI_SETL_FrmTMP_APPT_PDCT_OFI_SETL']) }}

SELECT
	APPT_PDCT_I,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtCSeApptPdctOfiSetlUpdateDS') }}