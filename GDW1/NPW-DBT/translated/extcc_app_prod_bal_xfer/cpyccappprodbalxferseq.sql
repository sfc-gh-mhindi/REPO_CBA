{{ config(materialized='view', tags=['ExtCC_APP_PROD_BAL_XFER']) }}

WITH CpyCCAppProdBalXferSeq AS (
	SELECT
		CC_APP_PROD_BAL_XFER_ID,
		BAL_XFER_OPTION_CAT_ID,
		XFER_AMT,
		BAL_XFER_INSTITUTION_CAT_ID,
		CC_APP_PROD_ID,
		CC_APP_ID
	FROM {{ ref('SrcCCAppProdBalXferSeq') }}
)

SELECT * FROM CpyCCAppProdBalXferSeq