{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_cse__chl__bus__hlmapp__premap', incremental_strategy='insert_overwrite', tags=['ExtBUS_HLM_APP']) }}

SELECT
	APP_ID,
	HLM_ACCOUNT_ID,
	ACCOUNT_NUMBER,
	CRIS_PRODUCT_ID,
	HLM_APP_TYPE_CAT_ID,
	DCHG_REAS_ID,
	HL_APP_PROD_ID,
	ORIG_ETL_D 
FROM {{ ref('FunMergeJournal') }}