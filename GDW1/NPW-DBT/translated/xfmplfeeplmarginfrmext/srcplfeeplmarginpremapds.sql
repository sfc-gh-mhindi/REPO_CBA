{{ config(materialized='view', tags=['XfmPlFeePlMarginFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__fee__margin__premap AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__fee__margin__premap")  }})
SrcPlFeePlMarginPremapDS AS (
	SELECT PL_FEE_ID,
		PL_APP_PROD_ID,
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
		PL_MARGIN_PL_MARGIN_ID,
		PL_MARGIN_MARGIN_AMT,
		PL_MARGIN_PL_FEE_ID,
		PL_MARGIN_PL_INT_RATE_ID,
		PL_MARGIN_MARGIN_REASON_CAT_ID,
		PL_MARGIN_PL_APP_PROD_ID,
		PL_FEE_FOUND_FLAG,
		PL_MARGIN_FOUND_FLAG,
		ORIG_ETL_D
	FROM _cba__app_csel4_csel4dev_dataset_cse__cpl__bus__fee__margin__premap
)

SELECT * FROM SrcPlFeePlMarginPremapDS