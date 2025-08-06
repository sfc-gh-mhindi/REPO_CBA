{{ config(materialized='view', tags=['ExtSmCaseState']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.SM_CASE_STATE_ID)) THEN (XfmSeparateRejects.SM_CASE_STATE_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.SM_CASE_STATE_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.SM_CASE_STATE_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		{{ ref('JoinSrcSortReject') }}.SM_CASE_STATE_ID_R AS SM_CASE_STATE_ID,
		{{ ref('JoinSrcSortReject') }}.SM_CASE_ID_R AS SM_CASE_ID,
		{{ ref('JoinSrcSortReject') }}.SM_STATE_CAT_ID_R AS SM_STATE_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.START_DATE_R AS START_DATE,
		{{ ref('JoinSrcSortReject') }}.END_DATE_R AS END_DATE,
		{{ ref('JoinSrcSortReject') }}.CREATED_BY_STAFF_NUMBER_R AS CREATED_BY_STAFF_NUMBER,
		{{ ref('JoinSrcSortReject') }}.STATE_CAUSED_BY_ACTION_ID_R AS STATE_CAUSED_BY_ACTION_ID,
		{{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'R'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec