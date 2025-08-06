{{ config(materialized='view', tags=['ExtHL_FEATURE_ATTR']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.HL_FEATURE_ATTR_ID)) THEN (XfmSeparateRejects.HL_FEATURE_ATTR_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.HL_FEATURE_ATTR_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.HL_FEATURE_ATTR_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		{{ ref('JoinSrcSortReject') }}.HL_FEATURE_ATTR_ID_R AS HL_FEATURE_ATTR_ID,
		{{ ref('JoinSrcSortReject') }}.FEATURE_TERM_R AS HL_FEATURE_TERM,
		{{ ref('JoinSrcSortReject') }}.FEATURE_AMOUNT_R AS HL_FEATURE_AMOUNT,
		{{ ref('JoinSrcSortReject') }}.FEATURE_BALANCE_R AS HL_FEATURE_BALANCE,
		{{ ref('JoinSrcSortReject') }}.FEATURE_FEE_R AS HL_FEATURE_FEE,
		{{ ref('JoinSrcSortReject') }}.FEATURE_SPEC_REPAY_R AS HL_FEATURE_SPEC_REPAY,
		{{ ref('JoinSrcSortReject') }}.FEATURE_EST_INT_AMT_R AS HL_FEATURE_EST_INT_AMT,
		{{ ref('JoinSrcSortReject') }}.FEATURE_DATE_R AS HL_FEATURE_DATE,
		{{ ref('JoinSrcSortReject') }}.FEATURE_COMMENT_R AS HL_FEATURE_COMMENT,
		{{ ref('JoinSrcSortReject') }}.HL_FEATURE_CAT_ID_R AS HL_FEATURE_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.HL_APP_PROD_ID_R AS HL_APP_PROD_ID,
		{{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'R'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec