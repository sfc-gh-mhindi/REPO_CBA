{{ config(materialized='view', tags=['ExtReasonSmCaseState']) }}

WITH JoinSrcSortReject AS (
	SELECT
		{{ ref('XfmCheckSmCaseStateIdNulls__OutCheckSmCaseStateIdNullsSorted') }}.SM_CASE_STATE_ID,
		{{ ref('XfmCheckSmCaseStateIdNulls__OutCheckSmCaseStateIdNullsSorted') }}.SCS_SM_CASE_ID,
		{{ ref('XfmCheckSmCaseStateIdNulls__OutCheckSmCaseStateIdNullsSorted') }}.SCS_SM_STATE_CAT_ID,
		{{ ref('XfmCheckSmCaseStateIdNulls__OutCheckSmCaseStateIdNullsSorted') }}.SCS_START_DATE,
		{{ ref('XfmCheckSmCaseStateIdNulls__OutCheckSmCaseStateIdNullsSorted') }}.SCS_END_DATE,
		{{ ref('XfmCheckSmCaseStateIdNulls__OutCheckSmCaseStateIdNullsSorted') }}.SCS_CREATED_BY_STAFF_NUMBER,
		{{ ref('XfmCheckSmCaseStateIdNulls__OutCheckSmCaseStateIdNullsSorted') }}.SCS_STATE_CAUSED_BY_ACTION_ID,
		{{ ref('XfmCheckSmCaseStateIdNulls__OutCheckSmCaseStateIdNullsSorted') }}.SCSR_SM_CASE_STATE_REASON_ID,
		{{ ref('XfmCheckSmCaseStateIdNulls__OutCheckSmCaseStateIdNullsSorted') }}.SCSR_SM_REASON_CAT_ID,
		{{ ref('XfmCheckSmCaseStateIdNulls__OutCheckSmCaseStateIdNullsSorted') }}.SM_CASE_STATE_REAS_FOUND_FLAG,
		{{ ref('XfmCheckSmCaseStateIdNulls__OutCheckSmCaseStateIdNullsSorted') }}.SM_CASE_STATE_FOUND_FLAG,
		{{ ref('CpyRejtSmCaseStateReasonRejectOra') }}.SM_CASE_STATE_ID AS SM_CASE_STATE_ID_R,
		{{ ref('CpyRejtSmCaseStateReasonRejectOra') }}.SCS_SM_CASE_ID_R,
		{{ ref('CpyRejtSmCaseStateReasonRejectOra') }}.SCS_SM_STATE_CAT_ID_R,
		{{ ref('CpyRejtSmCaseStateReasonRejectOra') }}.SCS_START_DATE_R,
		{{ ref('CpyRejtSmCaseStateReasonRejectOra') }}.SCS_END_DATE_R,
		{{ ref('CpyRejtSmCaseStateReasonRejectOra') }}.SCS_CREATED_BY_STAFF_NUMBER_R,
		{{ ref('CpyRejtSmCaseStateReasonRejectOra') }}.SCS_STATE_CAUSED_BY_ACTION_ID_R,
		{{ ref('CpyRejtSmCaseStateReasonRejectOra') }}.SCSR_SM_CASE_STATE_REASON_ID_R,
		{{ ref('CpyRejtSmCaseStateReasonRejectOra') }}.SCSR_SM_REASON_CAT_ID_R,
		{{ ref('CpyRejtSmCaseStateReasonRejectOra') }}.SM_CASE_STATE_REAS_FOUND_FLAG_R,
		{{ ref('CpyRejtSmCaseStateReasonRejectOra') }}.SM_CASE_STATE_FOUND_FLAG_R,
		{{ ref('CpyRejtSmCaseStateReasonRejectOra') }}.ORIG_ETL_D_R
	FROM {{ ref('XfmCheckSmCaseStateIdNulls__OutCheckSmCaseStateIdNullsSorted') }}
	OUTER JOIN {{ ref('CpyRejtSmCaseStateReasonRejectOra') }} ON {{ ref('XfmCheckSmCaseStateIdNulls__OutCheckSmCaseStateIdNullsSorted') }}.SM_CASE_STATE_ID = {{ ref('CpyRejtSmCaseStateReasonRejectOra') }}.SM_CASE_STATE_ID
)

SELECT * FROM JoinSrcSortReject