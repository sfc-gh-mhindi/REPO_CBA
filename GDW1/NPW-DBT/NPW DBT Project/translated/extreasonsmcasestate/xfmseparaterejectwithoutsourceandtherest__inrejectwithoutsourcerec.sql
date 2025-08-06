{{ config(materialized='view', tags=['ExtReasonSmCaseState']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.SM_CASE_STATE_ID)) THEN (XfmSeparateRejects.SM_CASE_STATE_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.SM_CASE_STATE_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.SM_CASE_STATE_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		{{ ref('JoinSrcSortReject') }}.SM_CASE_STATE_ID_R AS SM_CASE_STATE_ID,
		{{ ref('JoinSrcSortReject') }}.SCS_SM_CASE_ID_R AS SCS_SM_CASE_ID,
		{{ ref('JoinSrcSortReject') }}.SCS_SM_STATE_CAT_ID_R AS SCS_SM_STATE_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.SCS_START_DATE_R AS SCS_START_DATE,
		{{ ref('JoinSrcSortReject') }}.SCS_END_DATE_R AS SCS_END_DATE,
		{{ ref('JoinSrcSortReject') }}.SCS_CREATED_BY_STAFF_NUMBER_R AS SCS_CREATED_BY_STAFF_NUMBER,
		{{ ref('JoinSrcSortReject') }}.SCS_STATE_CAUSED_BY_ACTION_ID_R AS SCS_STATE_CAUSED_BY_ACTION_ID,
		{{ ref('JoinSrcSortReject') }}.SCSR_SM_CASE_STATE_REASON_ID_R AS SCSR_SM_CASE_STATE_REASON_ID,
		{{ ref('JoinSrcSortReject') }}.SCSR_SM_REASON_CAT_ID_R AS SCSR_SM_REASON_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.SM_CASE_STATE_REAS_FOUND_FLAG_R AS SM_CASE_STATE_REAS_FOUND_FLAG,
		{{ ref('JoinSrcSortReject') }}.SM_CASE_STATE_FOUND_FLAG_R AS SM_CASE_STATE_FOUND_FLAG,
		{{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'R'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec