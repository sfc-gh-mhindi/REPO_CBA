{{ config(materialized='view', tags=['ExtHL_APP_PROD_PURPOSE']) }}

WITH XfmCheckHlAppProdPurposeIdNulls__OutCheckHlAppProdPurposeIdNullsSorted AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmCheckHlAppProdPurposeIdNulls.HL_APP_PROD_PURPOSE_ID)) THEN (XfmCheckHlAppProdPurposeIdNulls.HL_APP_PROD_PURPOSE_ID) ELSE ""))) = 0) Then 'REJ2007' Else '',
		IFF(LEN(TRIM(IFF({{ ref('CpyHlAppProdPurposeSeq') }}.HL_APP_PROD_PURPOSE_ID IS NOT NULL, {{ ref('CpyHlAppProdPurposeSeq') }}.HL_APP_PROD_PURPOSE_ID, ''))) = 0, 'REJ2007', '') AS ErrorCode,
		HL_APP_PROD_PURPOSE_ID,
		HL_APP_PROD_ID,
		HL_LOAN_PURPOSE_CAT_ID,
		AMOUNT,
		MAIN_PURPOSE
	FROM {{ ref('CpyHlAppProdPurposeSeq') }}
	WHERE SUBSTRING(ErrorCode, 1, 3) <> 'REJ'
)

SELECT * FROM XfmCheckHlAppProdPurposeIdNulls__OutCheckHlAppProdPurposeIdNullsSorted