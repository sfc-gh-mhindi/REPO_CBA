{{ config(materialized='view', tags=['ExtHL_APP_PROD']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.HL_APP_PROD_ID)) THEN (XfmSeparateRejects.HL_APP_PROD_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.HL_APP_PROD_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.HL_APP_PROD_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		HL_APP_PROD_ID,
		PARENT_HL_APP_PROD_ID,
		HL_REPAYMENT_PERIOD_CAT_ID,
		AMOUNT,
		LOAN_TERM_MONTHS,
		ACCOUNT_NUMBER,
		TOTAL_LOAN_AMOUNT,
		HLS_FLAG,
		GDW_UPDATED_LDP_PAID_ON_AMOUNT,
		ETL_PROCESS_DT AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'S'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec