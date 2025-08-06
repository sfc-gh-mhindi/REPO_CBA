{{ config(materialized='view', tags=['ExtAppCon_TuApp']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.HL_APP_PROD_ID)) THEN (XfmSeparateRejects.HL_APP_PROD_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.HL_APP_PROD_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.HL_APP_PROD_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		SUBTYPE_CODE,
		HL_APP_PROD_ID,
		TU_APP_CONDITION_ID,
		TU_APP_CONDITION_CAT_ID,
		CONDITION_MET_DATE,
		{{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'S'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec