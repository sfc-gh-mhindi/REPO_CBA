{{ config(materialized='view', tags=['ExtSmCaseState']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.SM_CASE_STATE_ID)) THEN (XfmSeparateRejects.SM_CASE_STATE_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.SM_CASE_STATE_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.SM_CASE_STATE_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		SM_CASE_STATE_ID,
		SM_CASE_ID,
		SM_STATE_CAT_ID,
		START_DATE,
		END_DATE,
		CREATED_BY_STAFF_NUMBER,
		STATE_CAUSED_BY_ACTION_ID,
		ETL_PROCESS_DT AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'S'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec