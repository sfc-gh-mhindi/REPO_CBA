{{ config(materialized='view', tags=['ExtReasonSmCaseState']) }}

WITH CpyRejtSmCaseStateReasonRejectOra AS (
	SELECT
		SM_CASE_STATE_ID,
		{{ ref('SrcRejtSmCaseStateReasonRejectOra') }}.SCS_SM_CASE_ID AS SCS_SM_CASE_ID_R,
		{{ ref('SrcRejtSmCaseStateReasonRejectOra') }}.SCS_SM_STATE_CAT_ID AS SCS_SM_STATE_CAT_ID_R,
		{{ ref('SrcRejtSmCaseStateReasonRejectOra') }}.SCS_START_DATE AS SCS_START_DATE_R,
		{{ ref('SrcRejtSmCaseStateReasonRejectOra') }}.SCS_END_DATE AS SCS_END_DATE_R,
		{{ ref('SrcRejtSmCaseStateReasonRejectOra') }}.SCS_CREATED_BY_STAFF_NUMBER AS SCS_CREATED_BY_STAFF_NUMBER_R,
		{{ ref('SrcRejtSmCaseStateReasonRejectOra') }}.SCS_STATE_CAUSED_BY_ACTION_ID AS SCS_STATE_CAUSED_BY_ACTION_ID_R,
		{{ ref('SrcRejtSmCaseStateReasonRejectOra') }}.SCSR_SM_CASE_STATE_REASON_ID AS SCSR_SM_CASE_STATE_REASON_ID_R,
		{{ ref('SrcRejtSmCaseStateReasonRejectOra') }}.SCSR_SM_REASON_CAT_ID AS SCSR_SM_REASON_CAT_ID_R,
		{{ ref('SrcRejtSmCaseStateReasonRejectOra') }}.SM_CASE_STATE_REAS_FOUND_FLAG AS SM_CASE_STATE_REAS_FOUND_FLAG_R,
		{{ ref('SrcRejtSmCaseStateReasonRejectOra') }}.SM_CASE_STATE_FOUND_FLAG AS SM_CASE_STATE_FOUND_FLAG_R,
		{{ ref('SrcRejtSmCaseStateReasonRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R
	FROM {{ ref('SrcRejtSmCaseStateReasonRejectOra') }}
)

SELECT * FROM CpyRejtSmCaseStateReasonRejectOra