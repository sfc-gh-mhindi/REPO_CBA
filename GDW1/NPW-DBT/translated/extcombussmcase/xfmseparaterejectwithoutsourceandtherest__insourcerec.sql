{{ config(materialized='view', tags=['ExtComBusSmCase']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.SM_CASE_ID)) THEN (XfmSeparateRejects.SM_CASE_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.SM_CASE_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.SM_CASE_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		SM_CASE_ID,
		CREATED_TIMESTAMP,
		WIM_PROCESS_ID,
		ETL_PROCESS_DT AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'S'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec