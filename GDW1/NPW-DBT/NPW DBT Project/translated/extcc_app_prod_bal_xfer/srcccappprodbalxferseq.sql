{{ config(materialized='view', tags=['ExtCC_APP_PROD_BAL_XFER']) }}

WITH 
_cba__app_csel4_csel4dev_inprocess_cse__ccc__bus__app__prod__bal__xfer__cse__ccc__bus__app__prod__bal__xfer__20060616 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_inprocess_cse__ccc__bus__app__prod__bal__xfer__cse__ccc__bus__app__prod__bal__xfer__20060616")  }})
SrcCCAppProdBalXferSeq AS (
	SELECT RECORD_TYPE,
		MOD_TIMESTAMP,
		CC_APP_PROD_BAL_XFER_ID,
		BAL_XFER_OPTION_CAT_ID,
		XFER_AMT,
		BAL_XFER_INSTITUTION_CAT_ID,
		CC_APP_PROD_ID,
		CC_APP_ID,
		DUMMY
	FROM _cba__app_csel4_csel4dev_inprocess_cse__ccc__bus__app__prod__bal__xfer__cse__ccc__bus__app__prod__bal__xfer__20060616
)

SELECT * FROM SrcCCAppProdBalXferSeq