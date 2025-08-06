{{ config(materialized='view', tags=['MergePlIntRatePlMargin']) }}

WITH 
_cba__app_csel4_csel4dev_inprocess_cse__cpl__bus__int__rate__amt__cse__cpl__bus__int__rate__amt__margin__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_inprocess_cse__cpl__bus__int__rate__amt__cse__cpl__bus__int__rate__amt__margin__20060101")  }})
SrcPlIntRateAmtSeq AS (
	SELECT PL_INT_RATE_AMT_RECORD_TYPE,
		PL_INT_RATE_AMT_MOD_TIMESTAMP,
		PL_INT_RATE_AMT_PL_INT_RATE_AMT_ID,
		PL_INT_RATE_AMT_INT_RATE_AMT,
		PL_INT_RATE_AMT_SCHEDULE_DESC,
		PL_INT_RATE_AMT_PL_INT_RATE_CAT_ID,
		PL_INT_RATE_AMT_PL_INT_RATE_ID,
		PL_INT_RATE_AMT__DUMMY
	FROM _cba__app_csel4_csel4dev_inprocess_cse__cpl__bus__int__rate__amt__cse__cpl__bus__int__rate__amt__margin__20060101
)

SELECT * FROM SrcPlIntRateAmtSeq