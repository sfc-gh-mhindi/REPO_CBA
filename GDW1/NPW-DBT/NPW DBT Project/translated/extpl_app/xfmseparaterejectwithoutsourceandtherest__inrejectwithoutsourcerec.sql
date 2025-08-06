{{ config(materialized='view', tags=['ExtPL_APP']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.PL_APP_ID)) THEN (XfmSeparateRejects.PL_APP_ID) ELSE "")))) = 0 Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.PL_APP_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.PL_APP_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		{{ ref('JoinSrcSortReject') }}.PL_APP_ID_R AS PL_APP_ID,
		{{ ref('JoinSrcSortReject') }}.NOMINATED_BRANCH_ID_R AS NOMINATED_BRANCH_ID,
		{{ ref('JoinSrcSortReject') }}.PL_PACKAGE_CAT_ID_R AS PL_PACKAGE_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'R'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec