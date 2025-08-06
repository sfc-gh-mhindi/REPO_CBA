{{ config(materialized='incremental', alias='_cba__app_csel4_csel4__prd_dataset_tmp__cse__com__bus__ccl__chl__com__app__aset__adrs', incremental_strategy='insert_overwrite', tags=['XfmChlBusPrtyAdrs']) }}

SELECT
	ASET_I,
	ADRS_I,
	SRCE_SYST_C,
	EFFT_D,
	EXPY_D,
	EROR_SEQN_I,
	RUN_STRM,
	CHL_PRCP_SCUY_FLAG 
FROM {{ ref('dedup_aset_adrs') }}