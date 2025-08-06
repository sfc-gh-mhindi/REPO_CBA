{{ config(materialized='view', tags=['ExtPL_APP_PROD_PURP']) }}

WITH CpyRejtPlAppProdPurpRejectOra AS (
	SELECT
		PL_APP_PROD_PURP_ID,
		{{ ref('SrcRejtPlAppProdPurpRejectOra') }}.PL_APP_PROD_PURP_CAT_ID AS PL_APP_PROD_PURP_CAT_ID_R,
		{{ ref('SrcRejtPlAppProdPurpRejectOra') }}.AMT AS AMT_R,
		{{ ref('SrcRejtPlAppProdPurpRejectOra') }}.PL_APP_PROD_ID AS PL_APP_PROD_ID_R,
		{{ ref('SrcRejtPlAppProdPurpRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R
	FROM {{ ref('SrcRejtPlAppProdPurpRejectOra') }}
)

SELECT * FROM CpyRejtPlAppProdPurpRejectOra