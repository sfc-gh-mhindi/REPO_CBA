{{ config(materialized='view', tags=['MergeChlIntRateChlIntRatePercChlProdIntMarg']) }}

WITH 
_cba__app_csel4_csel4dev_inprocess_cse__chl__bus__prod__int__marg__cse__chl__bus__int__rt__perc__prod__int__marg__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_inprocess_cse__chl__bus__prod__int__marg__cse__chl__bus__int__rt__perc__prod__int__marg__20060101")  }})
SrcChlBusProdIntMarg AS (
	SELECT MARG_RECORD_TYPE,
		MARG_MOD_TIMESTAMP,
		MARG_HL_PROD_INT_MARGIN_ID,
		MARG_HL_INT_RATE_ID,
		MARG_HL_PROD_INT_MARGIN_CAT_ID,
		MARG_MARGIN_TYPE,
		MARG_MARGIN_DESC,
		MARG_MARGIN_CODE,
		MARG_MARGIN_AMOUNT,
		MARG_ADJ_AMT,
		MARG_DUMMY
	FROM _cba__app_csel4_csel4dev_inprocess_cse__chl__bus__prod__int__marg__cse__chl__bus__int__rt__perc__prod__int__marg__20060101
)

SELECT * FROM SrcChlBusProdIntMarg