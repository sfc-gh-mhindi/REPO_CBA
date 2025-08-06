{{ config(materialized='view', tags=['ExtReasonSmCaseState']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.SM_CASE_STATE_ID)) THEN (XfmSeparateRejects.SM_CASE_STATE_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.SM_CASE_STATE_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.SM_CASE_STATE_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		SM_CASE_STATE_ID,
		SCS_SM_CASE_ID,
		SCS_SM_STATE_CAT_ID,
		SCS_START_DATE,
		SCS_END_DATE,
		SCS_CREATED_BY_STAFF_NUMBER,
		SCS_STATE_CAUSED_BY_ACTION_ID,
		SCSR_SM_CASE_STATE_REASON_ID,
		SCSR_SM_REASON_CAT_ID,
		SM_CASE_STATE_REAS_FOUND_FLAG,
		SM_CASE_STATE_FOUND_FLAG,
		ETL_PROCESS_DT AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'S'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec