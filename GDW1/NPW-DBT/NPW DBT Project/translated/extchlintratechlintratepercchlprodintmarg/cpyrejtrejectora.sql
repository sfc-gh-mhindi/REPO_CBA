{{ config(materialized='view', tags=['ExtChlIntRateChlIntRatePercChlProdIntMarg']) }}

WITH CpyRejtRejectOra AS (
	SELECT
		HL_INT_RATE_ID,
		{{ ref('SrcRejtRejectOra') }}.RATE_HL_INT_RATE_ID AS RATE_HL_INT_RATE_ID_R,
		{{ ref('SrcRejtRejectOra') }}.RATE_HL_APP_PROD_ID AS RATE_HL_APP_PROD_ID_R,
		{{ ref('SrcRejtRejectOra') }}.RATE_CASS_INT_RATE_TYPE_CODE AS RATE_CASS_INT_RATE_TYPE_CODE_R,
		{{ ref('SrcRejtRejectOra') }}.RATE_RATE_SEQUENCE AS RATE_RATE_SEQUENCE_R,
		{{ ref('SrcRejtRejectOra') }}.RATE_DURATION AS RATE_DURATION_R,
		{{ ref('SrcRejtRejectOra') }}.PERC_HL_INT_RATE_ID AS PERC_HL_INT_RATE_ID_R,
		{{ ref('SrcRejtRejectOra') }}.PERC_SCHEDULE_RATE_TYPE AS PERC_SCHEDULE_RATE_TYPE_R,
		{{ ref('SrcRejtRejectOra') }}.PERC_RATE_PERCENT_1 AS PERC_RATE_PERCENT_1_R,
		{{ ref('SrcRejtRejectOra') }}.PERC_RATE_PERCENT_2 AS PERC_RATE_PERCENT_2_R,
		{{ ref('SrcRejtRejectOra') }}.MARG_HL_PROD_INT_MARGIN_ID AS MARG_HL_PROD_INT_MARGIN_ID_R,
		{{ ref('SrcRejtRejectOra') }}.MARG_HL_INT_RATE_ID AS MARG_HL_INT_RATE_ID_R,
		{{ ref('SrcRejtRejectOra') }}.MARG_HL_PROD_INT_MARGIN_CAT_ID AS MARG_HL_PROD_INT_MARGIN_CAT_ID_R,
		{{ ref('SrcRejtRejectOra') }}.MARG_MARGIN_TYPE AS MARG_MARGIN_TYPE_R,
		{{ ref('SrcRejtRejectOra') }}.MARG_MARGIN_DESC AS MARG_MARGIN_DESC_R,
		{{ ref('SrcRejtRejectOra') }}.MARG_MARGIN_CODE AS MARG_MARGIN_CODE_R,
		{{ ref('SrcRejtRejectOra') }}.MARG_MARGIN_AMOUNT AS MARG_MARGIN_AMOUNT_R,
		{{ ref('SrcRejtRejectOra') }}.MARG_ADJ_AMT AS MARG_ADJ_AMT_R,
		{{ ref('SrcRejtRejectOra') }}.RATE_FOUND_FLAG AS RATE_FOUND_FLAG_R,
		{{ ref('SrcRejtRejectOra') }}.PERC_FOUND_FLAG AS PERC_FOUND_FLAG_R,
		{{ ref('SrcRejtRejectOra') }}.MARG_FOUND_FLAG AS MARG_FOUND_FLAG_R,
		{{ ref('SrcRejtRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R
	FROM {{ ref('SrcRejtRejectOra') }}
)

SELECT * FROM CpyRejtRejectOra