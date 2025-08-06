{{ config(materialized='view', tags=['ExtComBusAppProdClientRole']) }}

WITH CpyRejtJournalRejectOra AS (
	SELECT
		APP_PROD_CLIENT_ROLE_ID,
		{{ ref('SrcRejtCCAppProdRejectOra') }}.ROLE_CAT_ID AS ROLE_CAT_ID_R,
		{{ ref('SrcRejtCCAppProdRejectOra') }}.CIF_CODE AS CIF_CODE_R,
		{{ ref('SrcRejtCCAppProdRejectOra') }}.APP_PROD_ID AS APP_PROD_ID_R,
		{{ ref('SrcRejtCCAppProdRejectOra') }}.SUBTYPE_CODE AS SUBTYPE_CODE_R,
		{{ ref('SrcRejtCCAppProdRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R
	FROM {{ ref('SrcRejtCCAppProdRejectOra') }}
)

SELECT * FROM CpyRejtJournalRejectOra