{{ config(materialized='view', tags=['ExtFaPropClnt']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.FA_PROPOSED_CLIENT_ID)) THEN (XfmSeparateRejects.FA_PROPOSED_CLIENT_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.FA_PROPOSED_CLIENT_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.FA_PROPOSED_CLIENT_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		{{ ref('JoinSrcSortReject') }}.FA_PROPOSED_CLIENT_ID_R AS FA_PROPOSED_CLIENT_ID,
		{{ ref('JoinSrcSortReject') }}.COIN_ENTITY_ID_R AS COIN_ENTITY_ID,
		{{ ref('JoinSrcSortReject') }}.CLIENT_CORRELATION_ID_R AS CLIENT_CORRELATION_ID,
		{{ ref('JoinSrcSortReject') }}.COIN_ENTITY_NAME_R AS COIN_ENTITY_NAME,
		{{ ref('JoinSrcSortReject') }}.FA_ENTITY_CAT_ID_R AS FA_ENTITY_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.FA_UNDERTAKING_ID_R AS FA_UNDERTAKING_ID,
		{{ ref('JoinSrcSortReject') }}.FA_PROPOSED_CLIENT_CAT_ID_R AS FA_PROPOSED_CLIENT_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R AS ORIG_ETL_D,
		3 AS change_code
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'R'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec