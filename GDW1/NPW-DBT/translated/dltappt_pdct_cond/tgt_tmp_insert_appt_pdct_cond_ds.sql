{{ config(materialized='incremental', alias='_cba__app_hlt_dev_dataset_appt__pdct__cond__i__cse__chl__bus__tu__app__cond__20071024', incremental_strategy='insert_overwrite', tags=['DltAPPT_PDCT_COND']) }}

SELECT
	APPT_PDCT_I,
	COND_C,
	EFFT_D,
	EXPY_D,
	APPT_PDCT_COND_MEET_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I 
FROM {{ ref('XFR_Split__To_Tmp_Insert_USR_TEM_DS') }}