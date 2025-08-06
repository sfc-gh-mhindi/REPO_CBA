{{ config(materialized='view', tags=['ExtFaUndertaking']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.FA_UNDERTAKING_ID)) THEN (XfmSeparateRejects.FA_UNDERTAKING_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.FA_UNDERTAKING_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.FA_UNDERTAKING_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		{{ ref('JoinSrcSortReject') }}.FA_UNDERTAKING_ID_R AS FA_UNDERTAKING_ID,
		{{ ref('JoinSrcSortReject') }}.PLANNING_GROUP_NAME_R AS PLANNING_GROUP_NAME,
		{{ ref('JoinSrcSortReject') }}.COIN_ADVICE_GROUP_ID_R AS COIN_ADVICE_GROUP_ID,
		{{ ref('JoinSrcSortReject') }}.ADVICE_GROUP_CORRELATION_ID_R AS ADVICE_GROUP_CORRELATION_ID,
		{{ ref('JoinSrcSortReject') }}.CREATED_DATE_R AS CREATED_DATE,
		{{ ref('JoinSrcSortReject') }}.CREATED_BY_STAFF_NUMBER_R AS CREATED_BY_STAFF_NUMBER,
		{{ ref('JoinSrcSortReject') }}.SM_CASE_ID_R AS SM_CASE_ID,
		{{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'R'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec