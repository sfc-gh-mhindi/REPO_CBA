{{ config(materialized='view', tags=['ExtFaPropClnt']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.FA_PROPOSED_CLIENT_ID)) THEN (XfmSeparateRejects.FA_PROPOSED_CLIENT_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.FA_PROPOSED_CLIENT_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.FA_PROPOSED_CLIENT_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		FA_PROPOSED_CLIENT_ID,
		COIN_ENTITY_ID,
		CLIENT_CORRELATION_ID,
		COIN_ENTITY_NAME,
		FA_ENTITY_CAT_ID,
		FA_UNDERTAKING_ID,
		FA_PROPOSED_CLIENT_CAT_ID,
		ETL_PROCESS_DT AS ORIG_ETL_D,
		change_code
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'S'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec