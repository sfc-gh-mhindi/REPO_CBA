{{ config(materialized='view', tags=['ExtPL_APP_PROD_PURP']) }}

WITH JoinSrcSortReject AS (
	SELECT
		{{ ref('XfmCheckPlAppProdPurpIdNulls__OutCheckPlAppProdPurpIdNullsSorted') }}.PL_APP_PROD_PURP_ID,
		{{ ref('XfmCheckPlAppProdPurpIdNulls__OutCheckPlAppProdPurpIdNullsSorted') }}.PL_APP_PROD_PURP_CAT_ID,
		{{ ref('XfmCheckPlAppProdPurpIdNulls__OutCheckPlAppProdPurpIdNullsSorted') }}.AMT,
		{{ ref('XfmCheckPlAppProdPurpIdNulls__OutCheckPlAppProdPurpIdNullsSorted') }}.PL_APP_PROD_ID,
		{{ ref('CpyRejtPlAppProdPurpRejectOra') }}.PL_APP_PROD_PURP_ID AS PL_APP_PROD_PURP_ID_R,
		{{ ref('CpyRejtPlAppProdPurpRejectOra') }}.PL_APP_PROD_PURP_CAT_ID_R,
		{{ ref('CpyRejtPlAppProdPurpRejectOra') }}.AMT_R,
		{{ ref('CpyRejtPlAppProdPurpRejectOra') }}.PL_APP_PROD_ID_R,
		{{ ref('CpyRejtPlAppProdPurpRejectOra') }}.ORIG_ETL_D_R
	FROM {{ ref('XfmCheckPlAppProdPurpIdNulls__OutCheckPlAppProdPurpIdNullsSorted') }}
	OUTER JOIN {{ ref('CpyRejtPlAppProdPurpRejectOra') }} ON {{ ref('XfmCheckPlAppProdPurpIdNulls__OutCheckPlAppProdPurpIdNullsSorted') }}.PL_APP_PROD_PURP_ID = {{ ref('CpyRejtPlAppProdPurpRejectOra') }}.PL_APP_PROD_PURP_ID
)

SELECT * FROM JoinSrcSortReject