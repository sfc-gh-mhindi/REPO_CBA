{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__hlmapp__appt__pdct__unid__paty', incremental_strategy='insert_overwrite', tags=['XfmHlmapp_Tpb']) }}

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
FROM {{ ref('XfmTrans__outAPPT_PDCT_UNID_PATY') }}