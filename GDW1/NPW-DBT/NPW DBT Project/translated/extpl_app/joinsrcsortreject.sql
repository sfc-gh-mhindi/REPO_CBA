{{ config(materialized='view', tags=['ExtPL_APP']) }}

WITH JoinSrcSortReject AS (
	SELECT
		{{ ref('XfmCheckPlAppIdNulls__OutCheckPlAppIdNullsSorted') }}.PL_APP_ID,
		{{ ref('XfmCheckPlAppIdNulls__OutCheckPlAppIdNullsSorted') }}.NOMINATED_BRANCH_ID,
		{{ ref('XfmCheckPlAppIdNulls__OutCheckPlAppIdNullsSorted') }}.PL_PACKAGE_CAT_ID,
		{{ ref('CpyRejtPlAppRejectOra') }}.PL_APP_ID AS PL_APP_ID_R,
		{{ ref('CpyRejtPlAppRejectOra') }}.NOMINATED_BRANCH_ID_R,
		{{ ref('CpyRejtPlAppRejectOra') }}.PL_PACKAGE_CAT_ID_R,
		{{ ref('CpyRejtPlAppRejectOra') }}.ORIG_ETL_D_R
	FROM {{ ref('XfmCheckPlAppIdNulls__OutCheckPlAppIdNullsSorted') }}
	OUTER JOIN {{ ref('CpyRejtPlAppRejectOra') }} ON {{ ref('XfmCheckPlAppIdNulls__OutCheckPlAppIdNullsSorted') }}.PL_APP_ID = {{ ref('CpyRejtPlAppRejectOra') }}.PL_APP_ID
)

SELECT * FROM JoinSrcSortReject