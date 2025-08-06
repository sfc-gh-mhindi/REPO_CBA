{{ config(materialized='view', tags=['ExtHL_APP_PROD_PURPOSE']) }}

WITH CpyRejtHlAppProdPurposeRejectOra AS (
	SELECT
		HL_APP_PROD_PURPOSE_ID,
		{{ ref('SrcRejtHlAppProdPurposeRejectOra') }}.HL_APP_PROD_ID AS HL_APP_PROD_ID_R,
		{{ ref('SrcRejtHlAppProdPurposeRejectOra') }}.HL_LOAN_PURPOSE_CAT_ID AS HL_LOAN_PURPOSE_CAT_ID_R,
		{{ ref('SrcRejtHlAppProdPurposeRejectOra') }}.AMOUNT AS AMOUNT_R,
		{{ ref('SrcRejtHlAppProdPurposeRejectOra') }}.MAIN_PURPOSE AS MAIN_PURPOSE_R,
		{{ ref('SrcRejtHlAppProdPurposeRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R
	FROM {{ ref('SrcRejtHlAppProdPurposeRejectOra') }}
)

SELECT * FROM CpyRejtHlAppProdPurposeRejectOra