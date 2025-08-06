{{ config(materialized='view', tags=['ExtPlFeePlMargin']) }}

WITH CpyRejtPlFeePlMarginRejectOra AS (
	SELECT
		PL_FEE_ID,
		{{ ref('SrcRejtPlFeePlMarginRejectOra') }}.PL_APP_PROD_ID AS PL_APP_PROD_ID_R,
		{{ ref('SrcRejtPlFeePlMarginRejectOra') }}.PL_FEE_PL_FEE_ID AS PL_FEE_PL_FEE_ID_R,
		{{ ref('SrcRejtPlFeePlMarginRejectOra') }}.PL_FEE_ADD_TO_TOTAL_FLAG AS PL_FEE_ADD_TO_TOTAL_FLAG_R,
		{{ ref('SrcRejtPlFeePlMarginRejectOra') }}.PL_FEE_FEE_AMT AS PL_FEE_FEE_AMT_R,
		{{ ref('SrcRejtPlFeePlMarginRejectOra') }}.PL_FEE_BASE_FEE_AMT AS PL_FEE_BASE_FEE_AMT_R,
		{{ ref('SrcRejtPlFeePlMarginRejectOra') }}.PL_FEE_PAY_FEE_AT_FUNDING_FLAG AS PL_FEE_PAY_FEE_AT_FUNDING_FLAG_R,
		{{ ref('SrcRejtPlFeePlMarginRejectOra') }}.PL_FEE_START_DATE AS PL_FEE_START_DATE_R,
		{{ ref('SrcRejtPlFeePlMarginRejectOra') }}.PL_FEE_PL_CAPITALIS_FEE_CAT_ID AS PL_FEE_PL_CAPITALIS_FEE_CAT_ID_R,
		{{ ref('SrcRejtPlFeePlMarginRejectOra') }}.PL_FEE_FEE_SCREEN_DESC AS PL_FEE_FEE_SCREEN_DESC_R,
		{{ ref('SrcRejtPlFeePlMarginRejectOra') }}.PL_FEE_FEE_DESC AS PL_FEE_FEE_DESC_R,
		{{ ref('SrcRejtPlFeePlMarginRejectOra') }}.PL_FEE_CASS_FEE_CODE AS PL_FEE_CASS_FEE_CODE_R,
		{{ ref('SrcRejtPlFeePlMarginRejectOra') }}.PL_FEE_CASS_FEE_KEY AS PL_FEE_CASS_FEE_KEY_R,
		{{ ref('SrcRejtPlFeePlMarginRejectOra') }}.PL_FEE_TOTAL_FEE_AMT AS PL_FEE_TOTAL_FEE_AMT_R,
		{{ ref('SrcRejtPlFeePlMarginRejectOra') }}.PL_FEE_PL_APP_PROD_ID AS PL_FEE_PL_APP_PROD_ID_R,
		{{ ref('SrcRejtPlFeePlMarginRejectOra') }}.PL_MARGIN_PL_MARGIN_ID AS PL_MARGIN_PL_MARGIN_ID_R,
		{{ ref('SrcRejtPlFeePlMarginRejectOra') }}.PL_MARGIN_MARGIN_AMT AS PL_MARGIN_MARGIN_AMT_R,
		{{ ref('SrcRejtPlFeePlMarginRejectOra') }}.PL_MARGIN_PL_FEE_ID AS PL_MARGIN_PL_FEE_ID_R,
		{{ ref('SrcRejtPlFeePlMarginRejectOra') }}.PL_MARGIN_PL_INT_RATE_ID AS PL_MARGIN_PL_INT_RATE_ID_R,
		{{ ref('SrcRejtPlFeePlMarginRejectOra') }}.PL_MARGIN_MARGIN_REASON_CAT_ID AS PL_MARGIN_MARGIN_REASON_CAT_ID_R,
		{{ ref('SrcRejtPlFeePlMarginRejectOra') }}.PL_MARGIN_PL_APP_PROD_ID AS PL_MARGIN_PL_APP_PROD_ID_R,
		{{ ref('SrcRejtPlFeePlMarginRejectOra') }}.PL_FEE_FOUND_FLAG AS PL_FEE_FOUND_FLAG_R,
		{{ ref('SrcRejtPlFeePlMarginRejectOra') }}.PL_MARGIN_FOUND_FLAG AS PL_MARGIN_FOUND_FLAG_R,
		{{ ref('SrcRejtPlFeePlMarginRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R
	FROM {{ ref('SrcRejtPlFeePlMarginRejectOra') }}
)

SELECT * FROM CpyRejtPlFeePlMarginRejectOra