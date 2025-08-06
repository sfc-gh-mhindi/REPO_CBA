{{ config(materialized='view', tags=['XfmCC_APP_PROD_BAL_XFERFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__ccc__bus__app__prod__bal__xfer__premap AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__ccc__bus__app__prod__bal__xfer__premap")  }})
SrcCCAppProdBalXferPremapDS AS (
	SELECT CC_APP_PROD_BAL_XFER_ID,
		BAL_XFER_OPTION_CAT_ID,
		XFER_AMT,
		BAL_XFER_INSTITUTION_CAT_ID,
		CC_APP_PROD_ID,
		CC_APP_ID,
		ORIG_ETL_D
	FROM _cba__app_csel4_csel4dev_dataset_cse__ccc__bus__app__prod__bal__xfer__premap
)

SELECT * FROM SrcCCAppProdBalXferPremapDS