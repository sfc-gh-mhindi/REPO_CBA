{{ config(materialized='view', tags=['ExtSmCaseState']) }}

WITH CpyRejtSmCaseStateReasonRejectOra AS (
	SELECT
		SM_CASE_STATE_ID,
		{{ ref('SrcRejtSmCaseStateReasonRejectOra') }}.SM_CASE_ID AS SM_CASE_ID_R,
		{{ ref('SrcRejtSmCaseStateReasonRejectOra') }}.SM_STATE_CAT_ID AS SM_STATE_CAT_ID_R,
		{{ ref('SrcRejtSmCaseStateReasonRejectOra') }}.START_DATE AS START_DATE_R,
		{{ ref('SrcRejtSmCaseStateReasonRejectOra') }}.END_DATE AS END_DATE_R,
		{{ ref('SrcRejtSmCaseStateReasonRejectOra') }}.CREATED_BY_STAFF_NUMBER AS CREATED_BY_STAFF_NUMBER_R,
		{{ ref('SrcRejtSmCaseStateReasonRejectOra') }}.STATE_CAUSED_BY_ACTION_ID AS STATE_CAUSED_BY_ACTION_ID_R,
		{{ ref('SrcRejtSmCaseStateReasonRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R
	FROM {{ ref('SrcRejtSmCaseStateReasonRejectOra') }}
)

SELECT * FROM CpyRejtSmCaseStateReasonRejectOra