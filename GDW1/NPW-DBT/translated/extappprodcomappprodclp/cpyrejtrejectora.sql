{{ config(materialized='view', tags=['ExtAppProdComAppProdClp']) }}

WITH CpyRejtRejectOra AS (
	SELECT
		APP_PROD_ID,
		{{ ref('SrcRejtRejectOra') }}.COM_SUBTYPE_CODE AS COM_SUBTYPE_CODE_R,
		{{ ref('SrcRejtRejectOra') }}.CAMPAIGN_CAT_ID AS CAMPAIGN_CAT_ID_R,
		{{ ref('SrcRejtRejectOra') }}.COM_APP_ID AS COM_APP_ID_R,
		{{ ref('SrcRejtRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R
	FROM {{ ref('SrcRejtRejectOra') }}
)

SELECT * FROM CpyRejtRejectOra