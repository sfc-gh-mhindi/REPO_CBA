{{ config(materialized='view', tags=['ExtPL_APP_PROD_PURP']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.PL_APP_PROD_PURP_ID)) THEN (XfmSeparateRejects.PL_APP_PROD_PURP_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.PL_APP_PROD_PURP_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.PL_APP_PROD_PURP_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		PL_APP_PROD_PURP_ID,
		PL_APP_PROD_PURP_CAT_ID,
		AMT,
		PL_APP_PROD_ID,
		ETL_PROCESS_DT AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'S'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec