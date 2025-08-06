{{ config(materialized='view', tags=['ExtHL_APP_PROD']) }}

WITH CpyRejtHlAppProdRejectOra AS (
	SELECT
		HL_APP_PROD_ID,
		{{ ref('SrcRejtHlAppProdRejectOra') }}.PARENT_HL_APP_PROD_ID AS PARENT_HL_APP_PROD_ID_R,
		{{ ref('SrcRejtHlAppProdRejectOra') }}.HL_REPAYMENT_PERIOD_CAT_ID AS HL_REPAYMENT_PERIOD_CAT_ID_R,
		{{ ref('SrcRejtHlAppProdRejectOra') }}.AMOUNT AS AMOUNT_R,
		{{ ref('SrcRejtHlAppProdRejectOra') }}.LOAN_TERM_MONTHS AS LOAN_TERM_MONTHS_R,
		{{ ref('SrcRejtHlAppProdRejectOra') }}.ACCOUNT_NUMBER AS ACCOUNT_NUMBER_R,
		{{ ref('SrcRejtHlAppProdRejectOra') }}.TOTAL_LOAN_AMOUNT AS TOTAL_LOAN_AMOUNT_R,
		{{ ref('SrcRejtHlAppProdRejectOra') }}.HLS_FLAG AS HLS_FLAG_R,
		{{ ref('SrcRejtHlAppProdRejectOra') }}.GDW_UPDATED_LDP_PAID_ON_AMOUNT AS GDW_UPDATED_LDP_PAID_ON_AMOUNT_R,
		{{ ref('SrcRejtHlAppProdRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R
	FROM {{ ref('SrcRejtHlAppProdRejectOra') }}
)

SELECT * FROM CpyRejtHlAppProdRejectOra