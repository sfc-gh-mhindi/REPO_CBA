{{ config(materialized='incremental', alias='_cba__app_hlt_dev_dataset_tmp__cse__chl__bus__tu__app__appt__pdct__unid__paty', incremental_strategy='insert_overwrite', tags=['XfmChlBusTuAPPFrmExt']) }}

SELECT
	APPT_PDCT_I,
	PATY_ROLE_C,
	SRCE_SYST_PATY_I,
	EFFT_D,
	SRCE_SYST_C,
	UNID_PATY_CATG_C,
	PATY_M,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	RUN_STRM 
FROM {{ ref('XfmBusinessRules__LdSrcApptPdctUnidPatyDS') }}