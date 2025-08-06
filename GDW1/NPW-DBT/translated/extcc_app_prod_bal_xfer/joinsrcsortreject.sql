{{ config(materialized='view', tags=['ExtCC_APP_PROD_BAL_XFER']) }}

WITH JoinSrcSortReject AS (
	SELECT
		{{ ref('XfmCheckCCAppProdBalXferIdNulls__OutCheckCCAppProdBalXferIdNullsSorted') }}.CC_APP_PROD_BAL_XFER_ID,
		{{ ref('XfmCheckCCAppProdBalXferIdNulls__OutCheckCCAppProdBalXferIdNullsSorted') }}.BAL_XFER_OPTION_CAT_ID,
		{{ ref('XfmCheckCCAppProdBalXferIdNulls__OutCheckCCAppProdBalXferIdNullsSorted') }}.XFER_AMT,
		{{ ref('XfmCheckCCAppProdBalXferIdNulls__OutCheckCCAppProdBalXferIdNullsSorted') }}.BAL_XFER_INSTITUTION_CAT_ID,
		{{ ref('XfmCheckCCAppProdBalXferIdNulls__OutCheckCCAppProdBalXferIdNullsSorted') }}.CC_APP_PROD_ID,
		{{ ref('XfmCheckCCAppProdBalXferIdNulls__OutCheckCCAppProdBalXferIdNullsSorted') }}.CC_APP_ID,
		{{ ref('CpyRejtCCAppProdBalXferRejectOra') }}.CC_APP_PROD_BAL_XFER_ID AS CC_APP_PROD_BAL_XFER_ID_R,
		{{ ref('CpyRejtCCAppProdBalXferRejectOra') }}.BAL_XFER_OPTION_CAT_ID_R,
		{{ ref('CpyRejtCCAppProdBalXferRejectOra') }}.XFER_AMT_R,
		{{ ref('CpyRejtCCAppProdBalXferRejectOra') }}.BAL_XFER_INSTITUTION_CAT_ID_R,
		{{ ref('CpyRejtCCAppProdBalXferRejectOra') }}.CC_APP_PROD_ID_R,
		{{ ref('CpyRejtCCAppProdBalXferRejectOra') }}.CC_APP_ID_R,
		{{ ref('CpyRejtCCAppProdBalXferRejectOra') }}.ORIG_ETL_D_R
	FROM {{ ref('XfmCheckCCAppProdBalXferIdNulls__OutCheckCCAppProdBalXferIdNullsSorted') }}
	OUTER JOIN {{ ref('CpyRejtCCAppProdBalXferRejectOra') }} ON {{ ref('XfmCheckCCAppProdBalXferIdNulls__OutCheckCCAppProdBalXferIdNullsSorted') }}.CC_APP_PROD_BAL_XFER_ID = {{ ref('CpyRejtCCAppProdBalXferRejectOra') }}.CC_APP_PROD_BAL_XFER_ID
)

SELECT * FROM JoinSrcSortReject