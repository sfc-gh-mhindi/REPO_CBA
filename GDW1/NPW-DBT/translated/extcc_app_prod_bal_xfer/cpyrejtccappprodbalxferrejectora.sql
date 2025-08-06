{{ config(materialized='view', tags=['ExtCC_APP_PROD_BAL_XFER']) }}

WITH CpyRejtCCAppProdBalXferRejectOra AS (
	SELECT
		CC_APP_PROD_BAL_XFER_ID,
		{{ ref('SrcRejtCCAppProdBalXferRejectOra') }}.BAL_XFER_OPTION_CAT_ID AS BAL_XFER_OPTION_CAT_ID_R,
		{{ ref('SrcRejtCCAppProdBalXferRejectOra') }}.XFER_AMT AS XFER_AMT_R,
		{{ ref('SrcRejtCCAppProdBalXferRejectOra') }}.BAL_XFER_INSTITUTION_CAT_ID AS BAL_XFER_INSTITUTION_CAT_ID_R,
		{{ ref('SrcRejtCCAppProdBalXferRejectOra') }}.CC_APP_PROD_ID AS CC_APP_PROD_ID_R,
		{{ ref('SrcRejtCCAppProdBalXferRejectOra') }}.CC_APP_ID AS CC_APP_ID_R,
		{{ ref('SrcRejtCCAppProdBalXferRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R
	FROM {{ ref('SrcRejtCCAppProdBalXferRejectOra') }}
)

SELECT * FROM CpyRejtCCAppProdBalXferRejectOra