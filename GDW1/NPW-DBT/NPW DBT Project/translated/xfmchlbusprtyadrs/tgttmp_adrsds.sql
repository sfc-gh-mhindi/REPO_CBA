{{ config(materialized='incremental', alias='_cba__app_csel4_csel4__prd_dataset_tmp__cse__com__bus__ccl__chl__com__app__adrs', incremental_strategy='insert_overwrite', tags=['XfmChlBusPrtyAdrs']) }}

SELECT
	ADRS_I,
	ADRS_TYPE_C,
	SRCE_SYST_C,
	ADRS_QLFY_C,
	SRCE_SYST_ADRS_I,
	SRCE_SYST_ADRS_SEQN_N,
	RUN_STRM 
FROM {{ ref('Dedup_adrs') }}