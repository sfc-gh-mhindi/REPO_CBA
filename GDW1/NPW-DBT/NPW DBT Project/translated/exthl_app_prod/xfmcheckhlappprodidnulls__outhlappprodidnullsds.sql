{{ config(materialized='view', tags=['ExtHL_APP_PROD']) }}

WITH XfmCheckHlAppProdIdNulls__OutHlAppProdIdNullsDS AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmCheckHlAppProdIdNulls.HL_APP_PROD_ID)) THEN (XfmCheckHlAppProdIdNulls.HL_APP_PROD_ID) ELSE ""))) = 0) Then 'REJ2007' Else '',
		IFF(LEN(TRIM(IFF({{ ref('CpyHlAppProdSeq') }}.HL_APP_PROD_ID IS NOT NULL, {{ ref('CpyHlAppProdSeq') }}.HL_APP_PROD_ID, ''))) = 0, 'REJ2007', '') AS ErrorCode,
		HL_APP_PROD_ID,
		PARENT_HL_APP_PROD_ID,
		HL_REPAYMENT_PERIOD_CAT_ID,
		AMOUNT,
		LOAN_TERM_MONTHS,
		ACCOUNT_NUMBER,
		TOTAL_LOAN_AMOUNT,
		HLS_FLAG,
		GDW_UPDATED_LDP_PAID_ON_AMOUNT,
		ETL_PROCESS_DT AS ETL_D,
		ETL_PROCESS_DT AS ORIG_ETL_D,
		ErrorCode AS EROR_C
	FROM {{ ref('CpyHlAppProdSeq') }}
	WHERE SUBSTRING(ErrorCode, 1, 3) = 'REJ'
)

SELECT * FROM XfmCheckHlAppProdIdNulls__OutHlAppProdIdNullsDS