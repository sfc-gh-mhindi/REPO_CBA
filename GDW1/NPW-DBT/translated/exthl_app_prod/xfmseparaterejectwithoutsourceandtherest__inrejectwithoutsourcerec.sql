{{ config(materialized='view', tags=['ExtHL_APP_PROD']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.HL_APP_PROD_ID)) THEN (XfmSeparateRejects.HL_APP_PROD_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.HL_APP_PROD_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.HL_APP_PROD_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		{{ ref('JoinSrcSortReject') }}.HL_APP_PROD_ID_R AS HL_APP_PROD_ID,
		{{ ref('JoinSrcSortReject') }}.PARENT_HL_APP_PROD_ID_R AS PARENT_HL_APP_PROD_ID,
		{{ ref('JoinSrcSortReject') }}.HL_REPAYMENT_PERIOD_CAT_ID_R AS HL_REPAYMENT_PERIOD_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.AMOUNT_R AS AMOUNT,
		{{ ref('JoinSrcSortReject') }}.LOAN_TERM_MONTHS_R AS LOAN_TERM_MONTHS,
		{{ ref('JoinSrcSortReject') }}.ACCOUNT_NUMBER_R AS ACCOUNT_NUMBER,
		{{ ref('JoinSrcSortReject') }}.TOTAL_LOAN_AMOUNT_R AS TOTAL_LOAN_AMOUNT,
		{{ ref('JoinSrcSortReject') }}.HLS_FLAG_R AS HLS_FLAG,
		{{ ref('JoinSrcSortReject') }}.GDW_UPDATED_LDP_PAID_ON_AMOUNT_R AS GDW_UPDATED_LDP_PAID_ON_AMOUNT,
		{{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'R'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec