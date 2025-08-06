{{ config(materialized='view', tags=['XfmCC_APP_PROD_BAL_XFERFrmExt']) }}

WITH CpyRename AS (
	SELECT
		CC_APP_PROD_BAL_XFER_ID,
		BAL_XFER_OPTION_CAT_ID,
		XFER_AMT,
		BAL_XFER_INSTITUTION_CAT_ID,
		CC_APP_PROD_ID,
		CC_APP_ID,
		ORIG_ETL_D
	FROM {{ ref('SrcCCAppProdBalXferPremapDS') }}
)

SELECT * FROM CpyRename