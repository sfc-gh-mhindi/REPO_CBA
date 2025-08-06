{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cse__com__bus__ccl__chl__com__app__premap', incremental_strategy='insert_overwrite', tags=['ExtChlBusPrtyAdrs']) }}

SELECT
	CHL_APP_HL_APP_ID,
	CHL_APP_SUBTYPE_CODE,
	CHL_APP_SECURITY_ID,
	CHL_ASSET_LIABILITY_ID,
	CHL_PRINCIPAL_SECURITY_FLAG,
	CHL_ADDRESS_LINE_1,
	CHL_ADDRESS_LINE_2,
	CHL_SUBURB,
	CHL_STATE,
	CHL_POSTCODE,
	CHL_DPID,
	CHL_COUNTRY_ID,
	ORIG_ETL_D 
FROM {{ ref('FunMergeSourceAndRejt') }}