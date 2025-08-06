{{ config(materialized='view', tags=['LdREJT_CSE_CHL_PRTY_ADRS']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__chl__bus__app__mapping__rejects AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__chl__bus__app__mapping__rejects")  }})
TgtCsePrtyAdrsRejectsDS AS (
	SELECT CHL_APP_HL_APP_ID,
		CHL_APP_SUBTYPE_CODE,
		CHL_APP_SECURITY_ID,
		CHL_ASSET_LIABILITY_ID,
		CHL_PRCP_SCUY_FLAG,
		CHL_ADDRESS_LINE_1,
		CHL_ADDRESS_LINE_2,
		CHL_SUBURB,
		CHL_STATE,
		CHL_POSTCODE,
		CHL_DPID,
		CHL_COUNTRY_ID,
		ETL_D,
		ORIG_ETL_D,
		EROR_C
	FROM _cba__app_csel4_csel4dev_dataset_cse__chl__bus__app__mapping__rejects
)

SELECT * FROM TgtCsePrtyAdrsRejectsDS