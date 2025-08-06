{{ config(materialized='view', tags=['ExtPlIntRatePlMargin']) }}

WITH CpyRejtPlIntRatePlMarginRejectOra AS (
	SELECT
		PL_INT_RATE_ID,
		{{ ref('SrcRejtPlIntRatePlMarginRejectOra') }}.PL_INT_RATE_PL_INT_RATE_ID AS PL_INT_RATE_PL_INT_RATE_ID_R,
		{{ ref('SrcRejtPlIntRatePlMarginRejectOra') }}.PL_INT_RATE_DOC_SEQ_NO AS PL_INT_RATE_DOC_SEQ_NO_R,
		{{ ref('SrcRejtPlIntRatePlMarginRejectOra') }}.PL_INT_RATE_CASS_MARGIN_AMT AS PL_INT_RATE_CASS_MARGIN_AMT_R,
		{{ ref('SrcRejtPlIntRatePlMarginRejectOra') }}.PL_INT_RATE_INT_RATE_TERM AS PL_INT_RATE_INT_RATE_TERM_R,
		{{ ref('SrcRejtPlIntRatePlMarginRejectOra') }}.PL_INT_RATE_INT_RATE_FREQ_ID AS PL_INT_RATE_INT_RATE_FREQUENCY_ID_R,
		{{ ref('SrcRejtPlIntRatePlMarginRejectOra') }}.PL_INT_RATE_VARIANT_CAT_ID AS PL_INT_RATE_VARIANT_CAT_ID_R,
		{{ ref('SrcRejtPlIntRatePlMarginRejectOra') }}.PL_INT_RATE_USAGE_CAT_ID AS PL_INT_RATE_USAGE_CAT_ID_R,
		{{ ref('SrcRejtPlIntRatePlMarginRejectOra') }}.PL_INT_RATE_PL_APP_PROD_ID AS PL_INT_RATE_PL_APP_PROD_ID_R,
		{{ ref('SrcRejtPlIntRatePlMarginRejectOra') }}.PL_MARGIN_PL_MARGIN_ID AS PL_MARGIN_PL_MARGIN_ID_R,
		{{ ref('SrcRejtPlIntRatePlMarginRejectOra') }}.PL_MARGIN_MARGIN_AMT AS PL_MARGIN_MARGIN_AMT_R,
		{{ ref('SrcRejtPlIntRatePlMarginRejectOra') }}.PL_MARGIN_PL_FEE_ID AS PL_MARGIN_PL_FEE_ID_R,
		{{ ref('SrcRejtPlIntRatePlMarginRejectOra') }}.PL_MARGIN_PL_INT_RATE_ID AS PL_MARGIN_PL_INT_RATE_ID_R,
		{{ ref('SrcRejtPlIntRatePlMarginRejectOra') }}.PL_MARGIN_MARGIN_RESN_CAT_ID AS PL_MARGIN_MARGIN_REASON_CAT_ID_R,
		{{ ref('SrcRejtPlIntRatePlMarginRejectOra') }}.PL_MARGIN_PL_APP_PROD_ID AS PL_MARGIN_PL_APP_PROD_ID_R,
		{{ ref('SrcRejtPlIntRatePlMarginRejectOra') }}.PL_INT_RATE_AMT_INT_RTE_AMT_2 AS PL_INT_RATE_AMT_INT_RATE_AMT_2_R,
		{{ ref('SrcRejtPlIntRatePlMarginRejectOra') }}.PL_INT_RATE_AMT_INT_RTCT_ID2 AS PL_INT_RATE_AMT_PL_INT_RATE_CAT_ID_2_R,
		{{ ref('SrcRejtPlIntRatePlMarginRejectOra') }}.PL_INT_RATE_AMT_PL_INT_RATE_ID AS PL_INT_RATE_AMT_PL_INT_RATE_ID_R,
		{{ ref('SrcRejtPlIntRatePlMarginRejectOra') }}.PL_INT_RATE_AMT_INT_RATE_AMT_1 AS PL_INT_RATE_AMT_INT_RATE_AMT_1_R,
		{{ ref('SrcRejtPlIntRatePlMarginRejectOra') }}.PL_INT_RATE_AMT_INT_RTCT_ID1 AS PL_INT_RATE_AMT_PL_INT_RATE_CAT_ID_1_R,
		{{ ref('SrcRejtPlIntRatePlMarginRejectOra') }}.PL_MARGIN_FOUND_FLAG AS PL_MARGIN_FOUND_FLAG_R,
		{{ ref('SrcRejtPlIntRatePlMarginRejectOra') }}.PL_INT_RATE_FOUND_FLAG AS PL_INT_RATE_FOUND_FLAG_R,
		{{ ref('SrcRejtPlIntRatePlMarginRejectOra') }}.PL_INT_RATE_AMT_FOUND_FLAG AS PL_INT_RATE_AMT_FOUND_FLAG_R,
		{{ ref('SrcRejtPlIntRatePlMarginRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R
	FROM {{ ref('SrcRejtPlIntRatePlMarginRejectOra') }}
)

SELECT * FROM CpyRejtPlIntRatePlMarginRejectOra