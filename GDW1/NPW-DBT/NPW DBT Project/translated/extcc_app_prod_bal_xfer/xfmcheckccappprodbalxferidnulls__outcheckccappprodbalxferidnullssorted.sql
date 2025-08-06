{{ config(materialized='view', tags=['ExtCC_APP_PROD_BAL_XFER']) }}

WITH XfmCheckCCAppProdBalXferIdNulls__OutCheckCCAppProdBalXferIdNullsSorted AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmCheckCCAppProdBalXferIdNulls.CC_APP_PROD_BAL_XFER_ID)) THEN (XfmCheckCCAppProdBalXferIdNulls.CC_APP_PROD_BAL_XFER_ID) ELSE ""))) = 0) Then 'REJ2003' Else '',
		IFF(LEN(TRIM(IFF({{ ref('CpyCCAppProdBalXferSeq') }}.CC_APP_PROD_BAL_XFER_ID IS NOT NULL, {{ ref('CpyCCAppProdBalXferSeq') }}.CC_APP_PROD_BAL_XFER_ID, ''))) = 0, 'REJ2003', '') AS ErrorCode,
		CC_APP_PROD_BAL_XFER_ID,
		BAL_XFER_OPTION_CAT_ID,
		XFER_AMT,
		BAL_XFER_INSTITUTION_CAT_ID,
		CC_APP_PROD_ID,
		CC_APP_ID
	FROM {{ ref('CpyCCAppProdBalXferSeq') }}
	WHERE SUBSTRING(ErrorCode, 1, 3) <> 'REJ'
)

SELECT * FROM XfmCheckCCAppProdBalXferIdNulls__OutCheckCCAppProdBalXferIdNullsSorted