{{ config(materialized='incremental', alias='_cba__app_hlt_dev_dataset_appt__pdct__cond__u__cse__chl__bus__tu__app__cond__20071024', incremental_strategy='insert_overwrite', tags=['DltAPPT_PDCT_COND']) }}

SELECT
	APPT_PDCT_I,
	COND_C,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XFR_Split__To_Tmp_Update_Paty_Int_Grup_DS') }}