{{ config(materialized='view', tags=['MergeChlIntRateChlIntRatePercChlProdIntMarg']) }}

WITH 
_cba__app_csel4_csel4dev_inprocess_cse__chl__bus__int__rate__cse__chl__bus__int__rt__perc__prod__int__marg__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_inprocess_cse__chl__bus__int__rate__cse__chl__bus__int__rt__perc__prod__int__marg__20060101")  }})
SrcChlBusIntRate AS (
	SELECT RATE_RECORD_TYPE,
		RATE_MOD_TIMESTAMP,
		RATE_HL_INT_RATE_ID,
		RATE_HL_APP_PROD_ID,
		RATE_CASS_INT_RATE_TYPE_CODE,
		RATE_RATE_SEQUENCE,
		RATE_DURATION,
		RATE_DUMMY
	FROM _cba__app_csel4_csel4dev_inprocess_cse__chl__bus__int__rate__cse__chl__bus__int__rt__perc__prod__int__marg__20060101
)

SELECT * FROM SrcChlBusIntRate