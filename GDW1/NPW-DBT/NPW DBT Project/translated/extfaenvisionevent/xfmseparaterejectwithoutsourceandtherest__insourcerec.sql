{{ config(materialized='view', tags=['ExtFaEnvisionEvent']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.FA_UNDERTAKING_ID)) THEN (XfmSeparateRejects.FA_UNDERTAKING_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.FA_UNDERTAKING_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.FA_UNDERTAKING_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		FA_ENVISION_EVENT_ID,
		FA_UNDERTAKING_ID,
		FA_ENVISION_EVENT_CAT_ID,
		CREATED_DATE,
		CREATED_BY_STAFF_NUMBER,
		COIN_REQUEST_ID,
		ETL_PROCESS_DT AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'S'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec