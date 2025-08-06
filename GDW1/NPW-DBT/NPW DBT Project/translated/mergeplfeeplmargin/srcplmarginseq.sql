{{ config(materialized='view', tags=['MergePlFeePlMargin']) }}

WITH 
_cba__app_csel4_csel4dev_inprocess_cse__cpl__bus__margin__cse__cpl__bus__fee__margin__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_inprocess_cse__cpl__bus__margin__cse__cpl__bus__fee__margin__20060101")  }})
SrcPlMarginSeq AS (
	SELECT PL_MARGIN_RECORD_TYPE,
		PL_MARGIN_MOD_TIMESTAMP,
		PL_MARGIN_PL_MARGIN_ID,
		PL_MARGIN_MARGIN_AMT,
		PL_MARGIN_PL_FEE_ID,
		PL_MARGIN_PL_INT_RATE_ID,
		PL_MARGIN_MARGIN_REASON_CAT_ID,
		PL_MARGIN_PL_APP_PROD_ID,
		PL_MARGIN_DUMMY
	FROM _cba__app_csel4_csel4dev_inprocess_cse__cpl__bus__margin__cse__cpl__bus__fee__margin__20060101
)

SELECT * FROM SrcPlMarginSeq