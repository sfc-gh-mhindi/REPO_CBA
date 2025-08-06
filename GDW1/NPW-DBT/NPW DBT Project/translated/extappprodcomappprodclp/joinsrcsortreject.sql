{{ config(materialized='view', tags=['ExtAppProdComAppProdClp']) }}

WITH JoinSrcSortReject AS (
	SELECT
		{{ ref('XfmCheckNulls__OutCheckNullsSorted') }}.APP_PROD_ID,
		{{ ref('XfmCheckNulls__OutCheckNullsSorted') }}.COM_SUBTYPE_CODE,
		{{ ref('XfmCheckNulls__OutCheckNullsSorted') }}.CAMPAIGN_CAT_ID,
		{{ ref('XfmCheckNulls__OutCheckNullsSorted') }}.COM_APP_ID,
		{{ ref('CpyRejtRejectOra') }}.APP_PROD_ID AS APP_PROD_ID_R,
		{{ ref('CpyRejtRejectOra') }}.COM_SUBTYPE_CODE_R,
		{{ ref('CpyRejtRejectOra') }}.CAMPAIGN_CAT_ID_R,
		{{ ref('CpyRejtRejectOra') }}.COM_APP_ID_R,
		{{ ref('CpyRejtRejectOra') }}.ORIG_ETL_D_R
	FROM {{ ref('XfmCheckNulls__OutCheckNullsSorted') }}
	OUTER JOIN {{ ref('CpyRejtRejectOra') }} ON {{ ref('XfmCheckNulls__OutCheckNullsSorted') }}.APP_PROD_ID = {{ ref('CpyRejtRejectOra') }}.APP_PROD_ID
)

SELECT * FROM JoinSrcSortReject