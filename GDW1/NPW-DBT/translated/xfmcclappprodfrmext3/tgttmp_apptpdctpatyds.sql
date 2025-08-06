{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_tmp__cse__ccl__bus__app__prod__appt__pdct__paty', incremental_strategy='insert_overwrite', tags=['XfmCclAppProdFrmExt3']) }}

SELECT
	APPT_PDCT_I,
	PATY_I,
	PATY_ROLE_C,
	EFFT_D,
	SRCE_SYST_C,
	SRCE_SYST_APPT_PDCT_PATY_I,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	RUN_STRM 
FROM {{ ref('XfmBusinessRules__ApptPdctPaty1') }}