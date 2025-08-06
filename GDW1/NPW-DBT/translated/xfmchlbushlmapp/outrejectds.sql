{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_cse__chl__bus__hlmapp__mapping__rejects', incremental_strategy='insert_overwrite', tags=['XfmChlBusHlmApp']) }}

SELECT
	HLM_APP_ID,
	HLM_ACCOUNT_ID,
	ACCOUNT_NUMBER,
	CRIS_PRODUCT_ID,
	HLM_APP_TYPE_CAT_ID,
	DISCHARGE_REASON_ID,
	HL_APP_PROD_ID,
	ETL_D,
	ORIG_ETL_D,
	EROR_C 
FROM {{ ref('XfmBusinessRules__Outrejectds') }}