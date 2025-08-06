{{ config(materialized='view', tags=['ExtHL_APP_PROD_PURPOSE']) }}

WITH CpyHlAppProdPurposeSeq AS (
	SELECT
		HL_APP_PROD_PURPOSE_ID,
		HL_APP_PROD_ID,
		HL_LOAN_PURPOSE_CAT_ID,
		AMOUNT,
		MAIN_PURPOSE
	FROM {{ ref('SrcHlAppProdPurposeSeq') }}
)

SELECT * FROM CpyHlAppProdPurposeSeq