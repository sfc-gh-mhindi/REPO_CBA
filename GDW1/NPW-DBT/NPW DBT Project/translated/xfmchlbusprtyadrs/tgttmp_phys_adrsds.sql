{{ config(materialized='incremental', alias='_cba__app_csel4_csel4__prd_dataset_tmp__cse__com__bus__ccl__chl__com__app__phys__adrs', incremental_strategy='insert_overwrite', tags=['XfmChlBusPrtyAdrs']) }}

SELECT
	ADRS_I,
	PHYS_ADRS_TYPE_C,
	ADRS_LINE_1_X,
	ADRS_LINE_2_X,
	SURB_X,
	CITY_X,
	PCOD_C,
	STAT_C,
	ISO_CNTY_C,
	EFFT_D,
	EXPY_D,
	RUN_STRM,
	CHL_PRCP_SCUY_FLAG 
FROM {{ ref('dedup_phys_adrs') }}