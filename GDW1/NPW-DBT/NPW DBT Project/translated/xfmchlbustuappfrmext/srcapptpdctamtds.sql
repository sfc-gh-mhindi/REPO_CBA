{{ config(materialized='incremental', alias='_cba__app_hlt_dev_dataset_tmp__cse__chl__bus__tu__app__2__appt__pdct__amt', incremental_strategy='insert_overwrite', tags=['XfmChlBusTuAPPFrmExt']) }}

SELECT
	APPT_PDCT_I,
	AMT_TYPE_C,
	EFFT_D,
	EXPY_D,
	CNCY_C,
	APPT_PDCT_A,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	RUN_STRM,
	DLTA_VERS 
FROM {{ ref('XfmBusinessRules__LdSrcTmpApptPdctAmtDS') }}