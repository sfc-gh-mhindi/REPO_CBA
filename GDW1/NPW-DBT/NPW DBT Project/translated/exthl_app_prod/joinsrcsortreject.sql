{{ config(materialized='view', tags=['ExtHL_APP_PROD']) }}

WITH JoinSrcSortReject AS (
	SELECT
		{{ ref('XfmCheckHlAppProdIdNulls__OutCheckHlAppProdIdNullsSorted') }}.HL_APP_PROD_ID,
		{{ ref('XfmCheckHlAppProdIdNulls__OutCheckHlAppProdIdNullsSorted') }}.PARENT_HL_APP_PROD_ID,
		{{ ref('XfmCheckHlAppProdIdNulls__OutCheckHlAppProdIdNullsSorted') }}.HL_REPAYMENT_PERIOD_CAT_ID,
		{{ ref('XfmCheckHlAppProdIdNulls__OutCheckHlAppProdIdNullsSorted') }}.AMOUNT,
		{{ ref('XfmCheckHlAppProdIdNulls__OutCheckHlAppProdIdNullsSorted') }}.LOAN_TERM_MONTHS,
		{{ ref('XfmCheckHlAppProdIdNulls__OutCheckHlAppProdIdNullsSorted') }}.ACCOUNT_NUMBER,
		{{ ref('XfmCheckHlAppProdIdNulls__OutCheckHlAppProdIdNullsSorted') }}.TOTAL_LOAN_AMOUNT,
		{{ ref('XfmCheckHlAppProdIdNulls__OutCheckHlAppProdIdNullsSorted') }}.HLS_FLAG,
		{{ ref('XfmCheckHlAppProdIdNulls__OutCheckHlAppProdIdNullsSorted') }}.GDW_UPDATED_LDP_PAID_ON_AMOUNT,
		{{ ref('CpyRejtHlAppProdRejectOra') }}.HL_APP_PROD_ID AS HL_APP_PROD_ID_R,
		{{ ref('CpyRejtHlAppProdRejectOra') }}.PARENT_HL_APP_PROD_ID_R,
		{{ ref('CpyRejtHlAppProdRejectOra') }}.HL_REPAYMENT_PERIOD_CAT_ID_R,
		{{ ref('CpyRejtHlAppProdRejectOra') }}.AMOUNT_R,
		{{ ref('CpyRejtHlAppProdRejectOra') }}.LOAN_TERM_MONTHS_R,
		{{ ref('CpyRejtHlAppProdRejectOra') }}.ACCOUNT_NUMBER_R,
		{{ ref('CpyRejtHlAppProdRejectOra') }}.TOTAL_LOAN_AMOUNT_R,
		{{ ref('CpyRejtHlAppProdRejectOra') }}.HLS_FLAG_R,
		{{ ref('CpyRejtHlAppProdRejectOra') }}.GDW_UPDATED_LDP_PAID_ON_AMOUNT_R,
		{{ ref('CpyRejtHlAppProdRejectOra') }}.ORIG_ETL_D_R
	FROM {{ ref('XfmCheckHlAppProdIdNulls__OutCheckHlAppProdIdNullsSorted') }}
	OUTER JOIN {{ ref('CpyRejtHlAppProdRejectOra') }} ON {{ ref('XfmCheckHlAppProdIdNulls__OutCheckHlAppProdIdNullsSorted') }}.HL_APP_PROD_ID = {{ ref('CpyRejtHlAppProdRejectOra') }}.HL_APP_PROD_ID
)

SELECT * FROM JoinSrcSortReject