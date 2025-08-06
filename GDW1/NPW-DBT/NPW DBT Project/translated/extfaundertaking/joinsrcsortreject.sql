{{ config(materialized='view', tags=['ExtFaUndertaking']) }}

WITH JoinSrcSortReject AS (
	SELECT
		{{ ref('XfmCheckIdNulls__OutCheckIdNullsSorted') }}.FA_UNDERTAKING_ID,
		{{ ref('XfmCheckIdNulls__OutCheckIdNullsSorted') }}.PLANNING_GROUP_NAME,
		{{ ref('XfmCheckIdNulls__OutCheckIdNullsSorted') }}.COIN_ADVICE_GROUP_ID,
		{{ ref('XfmCheckIdNulls__OutCheckIdNullsSorted') }}.ADVICE_GROUP_CORRELATION_ID,
		{{ ref('XfmCheckIdNulls__OutCheckIdNullsSorted') }}.CREATED_DATE,
		{{ ref('XfmCheckIdNulls__OutCheckIdNullsSorted') }}.CREATED_BY_STAFF_NUMBER,
		{{ ref('XfmCheckIdNulls__OutCheckIdNullsSorted') }}.SM_CASE_ID,
		{{ ref('CpyRejtRejectOra') }}.FA_UNDERTAKING_ID AS FA_UNDERTAKING_ID_R,
		{{ ref('CpyRejtRejectOra') }}.PLANNING_GROUP_NAME_R,
		{{ ref('CpyRejtRejectOra') }}.COIN_ADVICE_GROUP_ID_R,
		{{ ref('CpyRejtRejectOra') }}.ADVICE_GROUP_CORRELATION_ID_R,
		{{ ref('CpyRejtRejectOra') }}.CREATED_DATE_R,
		{{ ref('CpyRejtRejectOra') }}.CREATED_BY_STAFF_NUMBER_R,
		{{ ref('CpyRejtRejectOra') }}.SM_CASE_ID_R,
		{{ ref('CpyRejtRejectOra') }}.ORIG_ETL_D_R
	FROM {{ ref('XfmCheckIdNulls__OutCheckIdNullsSorted') }}
	OUTER JOIN {{ ref('CpyRejtRejectOra') }} ON {{ ref('XfmCheckIdNulls__OutCheckIdNullsSorted') }}.FA_UNDERTAKING_ID = {{ ref('CpyRejtRejectOra') }}.FA_UNDERTAKING_ID
)

SELECT * FROM JoinSrcSortReject