{{ config(materialized='view', tags=['MergePlFeePlMargin']) }}

WITH 
_cba__app_csel4_csel4dev_inprocess_cse__cpl__bus__fee__cse__cpl__bus__fee__margin__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_inprocess_cse__cpl__bus__fee__cse__cpl__bus__fee__margin__20060101")  }})
SrcPlFeeSeq AS (
	SELECT PL_FEE_RECORD_TYPE,
		PL_FEE_MOD_TIMESTAMP,
		PL_FEE_PL_FEE_ID,
		PL_FEE_ADD_TO_TOTAL_FLAG,
		PL_FEE_FEE_AMT,
		PL_FEE_BASE_FEE_AMT,
		PL_FEE_PAY_FEE_AT_FUNDING_FLAG,
		PL_FEE_START_DATE,
		PL_FEE_PL_CAPITALIS_FEE_CAT_ID,
		PL_FEE_FEE_SCREEN_DESC,
		PL_FEE_FEE_DESC,
		PL_FEE_CASS_FEE_CODE,
		PL_FEE_CASS_FEE_KEY,
		PL_FEE_TOTAL_FEE_AMT,
		PL_FEE_PL_APP_PROD_ID,
		PL_FEE_DUMMY
	FROM _cba__app_csel4_csel4dev_inprocess_cse__cpl__bus__fee__cse__cpl__bus__fee__margin__20060101
)

SELECT * FROM SrcPlFeeSeq