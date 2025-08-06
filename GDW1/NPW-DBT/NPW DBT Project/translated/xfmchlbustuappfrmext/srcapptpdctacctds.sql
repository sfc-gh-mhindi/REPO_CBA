{{ config(materialized='incremental', alias='_cba__app_hlt_dev_dataset_tmp__cse__chl__bus__tu__app__appt__pdct__acct', incremental_strategy='insert_overwrite', tags=['XfmChlBusTuAPPFrmExt']) }}

SELECT
	APPT_PDCT_I,
	ACCT_I,
	REL_TYPE_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	RUN_STRM 
FROM {{ ref('XfmBusinessRules__LdSrcApptPdctAcctDS') }}