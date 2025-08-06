{{ config(materialized='view', tags=['ExtComBusSmCase']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.SM_CASE_ID)) THEN (XfmSeparateRejects.SM_CASE_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.SM_CASE_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.SM_CASE_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		{{ ref('JoinSrcSortReject') }}.SM_CASE_ID_R AS SM_CASE_ID,
		{{ ref('JoinSrcSortReject') }}.CREATED_TIMESTAMP_R AS CREATED_TIMESTAMP,
		{{ ref('JoinSrcSortReject') }}.WIM_PROCESS_ID_R AS WIM_PROCESS_ID,
		{{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'R'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec