{{ config(materialized='view', tags=['ExtComBusAppProdClientRole']) }}

WITH JoinSrcSortReject AS (
	SELECT
		{{ ref('XfmCheckBusApPrdClntIdNulls__OutCheckBusApPrdClntIdNullsSorted') }}.APP_PROD_CLIENT_ROLE_ID,
		{{ ref('XfmCheckBusApPrdClntIdNulls__OutCheckBusApPrdClntIdNullsSorted') }}.ROLE_CAT_ID,
		{{ ref('XfmCheckBusApPrdClntIdNulls__OutCheckBusApPrdClntIdNullsSorted') }}.CIF_CODE,
		{{ ref('XfmCheckBusApPrdClntIdNulls__OutCheckBusApPrdClntIdNullsSorted') }}.APP_PROD_ID,
		{{ ref('XfmCheckBusApPrdClntIdNulls__OutCheckBusApPrdClntIdNullsSorted') }}.SUBTYPE_CODE,
		{{ ref('CpyRejtJournalRejectOra') }}.APP_PROD_CLIENT_ROLE_ID AS APP_PROD_CLIENT_ROLE_ID_R,
		{{ ref('CpyRejtJournalRejectOra') }}.ROLE_CAT_ID_R,
		{{ ref('CpyRejtJournalRejectOra') }}.CIF_CODE_R,
		{{ ref('CpyRejtJournalRejectOra') }}.APP_PROD_ID_R,
		{{ ref('CpyRejtJournalRejectOra') }}.SUBTYPE_CODE_R,
		{{ ref('CpyRejtJournalRejectOra') }}.ORIG_ETL_D_R
	FROM {{ ref('XfmCheckBusApPrdClntIdNulls__OutCheckBusApPrdClntIdNullsSorted') }}
	OUTER JOIN {{ ref('CpyRejtJournalRejectOra') }} ON {{ ref('XfmCheckBusApPrdClntIdNulls__OutCheckBusApPrdClntIdNullsSorted') }}.APP_PROD_CLIENT_ROLE_ID = {{ ref('CpyRejtJournalRejectOra') }}.APP_PROD_CLIENT_ROLE_ID
)

SELECT * FROM JoinSrcSortReject