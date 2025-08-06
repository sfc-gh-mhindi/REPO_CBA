{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_tmp__cse__cpl__bus__app__prod__appt__pdct__unid__paty', incremental_strategy='insert_overwrite', tags=['XfmPL_APP_PRODFrmExt']) }}

SELECT
	APPT_PDCT_I,
	SRCE_SYST_PATY_I,
	PATY_ROLE_C,
	UNID_PATY_CATG_C,
	SRCE_SYST_C,
	PATY_M,
	UNID_PATY_I,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	RUN_STRM 
FROM {{ ref('XfmBusinessRules__OutApptPdctUnidPatyDS') }}