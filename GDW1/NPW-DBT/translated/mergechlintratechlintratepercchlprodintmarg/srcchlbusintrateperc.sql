{{ config(materialized='view', tags=['MergeChlIntRateChlIntRatePercChlProdIntMarg']) }}

WITH 
_cba__app_csel4_csel4dev_inprocess_cse__chl__bus__int__rate__perc__cse__chl__bus__int__rt__perc__prod__int__marg__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_inprocess_cse__chl__bus__int__rate__perc__cse__chl__bus__int__rt__perc__prod__int__marg__20060101")  }})
SrcChlBusIntRatePerc AS (
	SELECT PERC_RECORD_TYPE,
		PERC_MOD_TIMESTAMP,
		PERC_HL_INT_RATE_PERCENT_ID,
		PERC_HL_INT_RATE_ID,
		PERC_RATE_PERCENT,
		PERC_SCHEDULE_RATE_TYPE,
		PERC_DUMMY
	FROM _cba__app_csel4_csel4dev_inprocess_cse__chl__bus__int__rate__perc__cse__chl__bus__int__rt__perc__prod__int__marg__20060101
)

SELECT * FROM SrcChlBusIntRatePerc