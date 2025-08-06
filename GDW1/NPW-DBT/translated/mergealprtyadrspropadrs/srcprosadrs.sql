{{ config(materialized='view', tags=['MergeAlPrtyAdrsPropAdrs']) }}

WITH 
_cba__app_csel4_csel4dev_inprocess_chl__bus__asst__lbty__prop__adrs__cse__com__bus__ptry__adrs__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_inprocess_chl__bus__asst__lbty__prop__adrs__cse__com__bus__ptry__adrs__20060101")  }})
SrcProsAdrs AS (
	SELECT CHL_APP_RECORD_TYPE,
		CHL_APP_MOD_TIMESTAMP,
		CHL_APP_HL_APP_ID,
		CHL_APP_SUBTYPE_CODE,
		CHL_APP_SECURITY_ID,
		CHL_ASSET_LIABILITY_ID,
		CHL_PRINCIPAL_SECURITY_FLAG,
		ASSET_LIABILITY_PROPOSED_ID,
		CHL_ADDRESS_LINE_1,
		CHL_ADDRESS_LINE_2,
		CHL_SUBURB,
		CHL_STATE,
		CHL_POSTCODE,
		CHL_DPID,
		CHL_COUNTRY_ID,
		CHL_DESCRIPTION,
		DUMMY
	FROM _cba__app_csel4_csel4dev_inprocess_chl__bus__asst__lbty__prop__adrs__cse__com__bus__ptry__adrs__20060101
)

SELECT * FROM SrcProsAdrs