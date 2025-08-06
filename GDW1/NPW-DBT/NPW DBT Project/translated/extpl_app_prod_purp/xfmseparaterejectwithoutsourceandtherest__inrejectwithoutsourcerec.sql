{{ config(materialized='view', tags=['ExtPL_APP_PROD_PURP']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.PL_APP_PROD_PURP_ID)) THEN (XfmSeparateRejects.PL_APP_PROD_PURP_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.PL_APP_PROD_PURP_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.PL_APP_PROD_PURP_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		{{ ref('JoinSrcSortReject') }}.PL_APP_PROD_PURP_ID_R AS PL_APP_PROD_PURP_ID,
		{{ ref('JoinSrcSortReject') }}.PL_APP_PROD_PURP_CAT_ID_R AS PL_APP_PROD_PURP_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.AMT_R AS AMT,
		{{ ref('JoinSrcSortReject') }}.PL_APP_PROD_ID_R AS PL_APP_PROD_ID,
		{{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'R'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec