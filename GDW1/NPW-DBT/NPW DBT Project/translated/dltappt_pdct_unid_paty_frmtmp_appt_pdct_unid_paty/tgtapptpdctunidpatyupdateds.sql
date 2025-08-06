{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__pdct__unid__paty__u__cse__chl__bus__hlm__app__20100614', incremental_strategy='insert_overwrite', tags=['DltAPPT_PDCT_UNID_PATY_FrmTMP_APPT_PDCT_UNID_PATY']) }}

SELECT
	APPT_PDCT_I,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptPdctUnidPatyUpdateDS') }}