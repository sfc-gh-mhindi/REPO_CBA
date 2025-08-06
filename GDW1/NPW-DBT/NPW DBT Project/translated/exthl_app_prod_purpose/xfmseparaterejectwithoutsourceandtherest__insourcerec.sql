{{ config(materialized='view', tags=['ExtHL_APP_PROD_PURPOSE']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.HL_APP_PROD_PURPOSE_ID)) THEN (XfmSeparateRejects.HL_APP_PROD_PURPOSE_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.HL_APP_PROD_PURPOSE_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.HL_APP_PROD_PURPOSE_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		HL_APP_PROD_PURPOSE_ID,
		HL_APP_PROD_ID,
		HL_LOAN_PURPOSE_CAT_ID,
		AMOUNT,
		MAIN_PURPOSE,
		ETL_PROCESS_DT AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'S'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec