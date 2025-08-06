{{ config(materialized='view', tags=['ExtFaPropClnt']) }}

WITH JoinSrcSortReject AS (
	SELECT
		{{ ref('XfmCheckIdNulls__OutCheckIdNullsSorted') }}.FA_PROPOSED_CLIENT_ID,
		{{ ref('XfmCheckIdNulls__OutCheckIdNullsSorted') }}.COIN_ENTITY_ID,
		{{ ref('XfmCheckIdNulls__OutCheckIdNullsSorted') }}.CLIENT_CORRELATION_ID,
		{{ ref('XfmCheckIdNulls__OutCheckIdNullsSorted') }}.COIN_ENTITY_NAME,
		{{ ref('XfmCheckIdNulls__OutCheckIdNullsSorted') }}.FA_ENTITY_CAT_ID,
		{{ ref('XfmCheckIdNulls__OutCheckIdNullsSorted') }}.FA_UNDERTAKING_ID,
		{{ ref('XfmCheckIdNulls__OutCheckIdNullsSorted') }}.FA_PROPOSED_CLIENT_CAT_ID,
		{{ ref('XfmCheckIdNulls__OutCheckIdNullsSorted') }}.change_code,
		{{ ref('CpyRejtRejectOra') }}.FA_PROPOSED_CLIENT_ID AS FA_PROPOSED_CLIENT_ID_R,
		{{ ref('CpyRejtRejectOra') }}.COIN_ENTITY_ID_R,
		{{ ref('CpyRejtRejectOra') }}.CLIENT_CORRELATION_ID_R,
		{{ ref('CpyRejtRejectOra') }}.COIN_ENTITY_NAME_R,
		{{ ref('CpyRejtRejectOra') }}.FA_ENTITY_CAT_ID_R,
		{{ ref('CpyRejtRejectOra') }}.FA_UNDERTAKING_ID AS FA_UNDERTAKING_ID_R,
		{{ ref('CpyRejtRejectOra') }}.FA_PROPOSED_CLIENT_CAT_ID_R,
		{{ ref('CpyRejtRejectOra') }}.ORIG_ETL_D_R
	FROM {{ ref('XfmCheckIdNulls__OutCheckIdNullsSorted') }}
	OUTER JOIN {{ ref('CpyRejtRejectOra') }} ON {{ ref('XfmCheckIdNulls__OutCheckIdNullsSorted') }}.FA_UNDERTAKING_ID = {{ ref('CpyRejtRejectOra') }}.FA_UNDERTAKING_ID
	AND {{ ref('XfmCheckIdNulls__OutCheckIdNullsSorted') }}.FA_PROPOSED_CLIENT_ID = {{ ref('CpyRejtRejectOra') }}.FA_PROPOSED_CLIENT_ID
)

SELECT * FROM JoinSrcSortReject