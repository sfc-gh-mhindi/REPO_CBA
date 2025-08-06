{{ config(materialized='view', tags=['ExtFaEnvisionEvent']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.FA_UNDERTAKING_ID)) THEN (XfmSeparateRejects.FA_UNDERTAKING_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.FA_UNDERTAKING_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.FA_UNDERTAKING_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		{{ ref('JoinSrcSortReject') }}.FA_ENVISION_EVENT_ID_R AS FA_ENVISION_EVENT_ID,
		{{ ref('JoinSrcSortReject') }}.FA_UNDERTAKING_ID_R AS FA_UNDERTAKING_ID,
		{{ ref('JoinSrcSortReject') }}.FA_ENVISION_EVENT_CAT_ID_R AS FA_ENVISION_EVENT_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.CREATED_DATE_R AS CREATED_DATE,
		{{ ref('JoinSrcSortReject') }}.CREATED_BY_STAFF_NUMBER_R AS CREATED_BY_STAFF_NUMBER,
		{{ ref('JoinSrcSortReject') }}.COIN_REQUEST_ID_R AS COIN_REQUEST_ID,
		{{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'R'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec