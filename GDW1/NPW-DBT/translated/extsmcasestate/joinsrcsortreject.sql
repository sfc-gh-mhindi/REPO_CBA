{{ config(materialized='view', tags=['ExtSmCaseState']) }}

WITH JoinSrcSortReject AS (
	SELECT
		{{ ref('XfmCheckSmCaseStateIdNulls__OutCheckSmCaseStateIdNullsSorted') }}.SM_CASE_STATE_ID,
		{{ ref('XfmCheckSmCaseStateIdNulls__OutCheckSmCaseStateIdNullsSorted') }}.SM_CASE_ID,
		{{ ref('XfmCheckSmCaseStateIdNulls__OutCheckSmCaseStateIdNullsSorted') }}.SM_STATE_CAT_ID,
		{{ ref('XfmCheckSmCaseStateIdNulls__OutCheckSmCaseStateIdNullsSorted') }}.START_DATE,
		{{ ref('XfmCheckSmCaseStateIdNulls__OutCheckSmCaseStateIdNullsSorted') }}.END_DATE,
		{{ ref('XfmCheckSmCaseStateIdNulls__OutCheckSmCaseStateIdNullsSorted') }}.CREATED_BY_STAFF_NUMBER,
		{{ ref('XfmCheckSmCaseStateIdNulls__OutCheckSmCaseStateIdNullsSorted') }}.STATE_CAUSED_BY_ACTION_ID,
		{{ ref('CpyRejtSmCaseStateReasonRejectOra') }}.SM_CASE_STATE_ID AS SM_CASE_STATE_ID_R,
		{{ ref('CpyRejtSmCaseStateReasonRejectOra') }}.SM_CASE_ID_R,
		{{ ref('CpyRejtSmCaseStateReasonRejectOra') }}.SM_STATE_CAT_ID_R,
		{{ ref('CpyRejtSmCaseStateReasonRejectOra') }}.START_DATE_R,
		{{ ref('CpyRejtSmCaseStateReasonRejectOra') }}.END_DATE_R,
		{{ ref('CpyRejtSmCaseStateReasonRejectOra') }}.CREATED_BY_STAFF_NUMBER_R,
		{{ ref('CpyRejtSmCaseStateReasonRejectOra') }}.STATE_CAUSED_BY_ACTION_ID_R,
		{{ ref('CpyRejtSmCaseStateReasonRejectOra') }}.ORIG_ETL_D_R
	FROM {{ ref('XfmCheckSmCaseStateIdNulls__OutCheckSmCaseStateIdNullsSorted') }}
	OUTER JOIN {{ ref('CpyRejtSmCaseStateReasonRejectOra') }} ON {{ ref('XfmCheckSmCaseStateIdNulls__OutCheckSmCaseStateIdNullsSorted') }}.SM_CASE_STATE_ID = {{ ref('CpyRejtSmCaseStateReasonRejectOra') }}.SM_CASE_STATE_ID
)

SELECT * FROM JoinSrcSortReject