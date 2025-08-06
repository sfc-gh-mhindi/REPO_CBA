{{ config(materialized='view', tags=['ExtFaEnvisionEvent']) }}

WITH JoinSrcSortReject AS (
	SELECT
		{{ ref('XfmCheckIdNulls__OutCheckIdNullsSorted') }}.FA_ENVISION_EVENT_ID,
		{{ ref('XfmCheckIdNulls__OutCheckIdNullsSorted') }}.FA_UNDERTAKING_ID,
		{{ ref('XfmCheckIdNulls__OutCheckIdNullsSorted') }}.FA_ENVISION_EVENT_CAT_ID,
		{{ ref('XfmCheckIdNulls__OutCheckIdNullsSorted') }}.CREATED_DATE,
		{{ ref('XfmCheckIdNulls__OutCheckIdNullsSorted') }}.CREATED_BY_STAFF_NUMBER,
		{{ ref('XfmCheckIdNulls__OutCheckIdNullsSorted') }}.COIN_REQUEST_ID,
		{{ ref('CpyRejtRejectOra') }}.FA_ENVISION_EVENT_ID AS FA_ENVISION_EVENT_ID_R,
		{{ ref('CpyRejtRejectOra') }}.FA_UNDERTAKING_ID_R,
		{{ ref('CpyRejtRejectOra') }}.FA_ENVISION_EVENT_CAT_ID_R,
		{{ ref('CpyRejtRejectOra') }}.CREATED_DATE_R,
		{{ ref('CpyRejtRejectOra') }}.CREATED_BY_STAFF_NUMBER_R,
		{{ ref('CpyRejtRejectOra') }}.COIN_REQUEST_ID_R,
		{{ ref('CpyRejtRejectOra') }}.ORIG_ETL_D_R
	FROM {{ ref('XfmCheckIdNulls__OutCheckIdNullsSorted') }}
	OUTER JOIN {{ ref('CpyRejtRejectOra') }} ON {{ ref('XfmCheckIdNulls__OutCheckIdNullsSorted') }}.FA_ENVISION_EVENT_ID = {{ ref('CpyRejtRejectOra') }}.FA_ENVISION_EVENT_ID
)

SELECT * FROM JoinSrcSortReject