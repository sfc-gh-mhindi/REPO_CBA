{{ config(materialized='view', tags=['ExtAppCon_TuApp']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.HL_APP_PROD_ID)) THEN (XfmSeparateRejects.HL_APP_PROD_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.HL_APP_PROD_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.HL_APP_PROD_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		{{ ref('JoinSrcSortReject') }}.TU_APP_CONDITION_ID_R AS TU_APP_CONDITION_ID,
		{{ ref('JoinSrcSortReject') }}.TU_APP_CONDITION_CAT_ID_R AS TU_APP_CONDITION_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.CONDITION_MET_DATE_R AS CONDITION_MET_DATE,
		{{ ref('JoinSrcSortReject') }}.SUBTYPE_CODE_R AS SUBTYPE_CODE,
		{{ ref('JoinSrcSortReject') }}.HL_APP_PROD_ID_R AS HL_APP_PROD_ID,
		{{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'R'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec