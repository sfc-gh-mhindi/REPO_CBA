{{ config(materialized='view', tags=['ExtHL_APP_PROD_PURPOSE']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.HL_APP_PROD_PURPOSE_ID)) THEN (XfmSeparateRejects.HL_APP_PROD_PURPOSE_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.HL_APP_PROD_PURPOSE_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.HL_APP_PROD_PURPOSE_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		{{ ref('JoinSrcSortReject') }}.HL_APP_PROD_PURPOSE_ID_R AS HL_APP_PROD_PURPOSE_ID,
		{{ ref('JoinSrcSortReject') }}.HL_APP_PROD_ID_R AS HL_APP_PROD_ID,
		{{ ref('JoinSrcSortReject') }}.HL_LOAN_PURPOSE_CAT_ID_R AS HL_LOAN_PURPOSE_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.AMOUNT_R AS AMOUNT,
		{{ ref('JoinSrcSortReject') }}.MAIN_PURPOSE_R AS MAIN_PURPOSE,
		{{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'R'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec