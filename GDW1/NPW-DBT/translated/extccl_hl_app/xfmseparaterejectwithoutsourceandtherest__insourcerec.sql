{{ config(materialized='view', tags=['ExtCCL_HL_APP']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.CCL_HL_APP_ID)) THEN (XfmSeparateRejects.CCL_HL_APP_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.CCL_HL_APP_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.CCL_HL_APP_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		CCL_HL_APP_ID,
		CCL_APP_ID,
		HL_APP_ID,
		LMI_AMT,
		HL_PACKAGE_CAT_ID,
		ETL_PROCESS_DT AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'S'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec