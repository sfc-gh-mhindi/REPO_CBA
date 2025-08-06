{{ config(materialized='view', tags=['ExtFaUndertaking']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.FA_UNDERTAKING_ID)) THEN (XfmSeparateRejects.FA_UNDERTAKING_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.FA_UNDERTAKING_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.FA_UNDERTAKING_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		FA_UNDERTAKING_ID,
		PLANNING_GROUP_NAME,
		COIN_ADVICE_GROUP_ID,
		ADVICE_GROUP_CORRELATION_ID,
		CREATED_DATE,
		CREATED_BY_STAFF_NUMBER,
		SM_CASE_ID,
		ETL_PROCESS_DT AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'S'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec