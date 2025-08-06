{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cse__com__bus__ptry__adrs__merge__20060101', incremental_strategy='insert_overwrite', tags=['MergeAlPrtyAdrsPropAdrs']) }}

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
	CHL_COUNTRY_ID 
FROM {{ ref('Merge') }}