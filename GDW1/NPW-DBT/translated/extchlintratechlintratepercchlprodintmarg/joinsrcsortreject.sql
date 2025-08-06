{{ config(materialized='view', tags=['ExtChlIntRateChlIntRatePercChlProdIntMarg']) }}

WITH JoinSrcSortReject AS (
	SELECT
		{{ ref('XfmCheckNulls__OutCheckNullsSorted') }}.HL_INT_RATE_ID,
		{{ ref('XfmCheckNulls__OutCheckNullsSorted') }}.RATE_HL_INT_RATE_ID,
		{{ ref('XfmCheckNulls__OutCheckNullsSorted') }}.RATE_HL_APP_PROD_ID,
		{{ ref('XfmCheckNulls__OutCheckNullsSorted') }}.RATE_CASS_INT_RATE_TYPE_CODE,
		{{ ref('XfmCheckNulls__OutCheckNullsSorted') }}.RATE_RATE_SEQUENCE,
		{{ ref('XfmCheckNulls__OutCheckNullsSorted') }}.RATE_DURATION,
		{{ ref('XfmCheckNulls__OutCheckNullsSorted') }}.PERC_HL_INT_RATE_ID,
		{{ ref('XfmCheckNulls__OutCheckNullsSorted') }}.PERC_SCHEDULE_RATE_TYPE,
		{{ ref('XfmCheckNulls__OutCheckNullsSorted') }}.PERC_RATE_PERCENT_1,
		{{ ref('XfmCheckNulls__OutCheckNullsSorted') }}.PERC_RATE_PERCENT_2,
		{{ ref('XfmCheckNulls__OutCheckNullsSorted') }}.MARG_HL_PROD_INT_MARGIN_ID,
		{{ ref('XfmCheckNulls__OutCheckNullsSorted') }}.MARG_HL_INT_RATE_ID,
		{{ ref('XfmCheckNulls__OutCheckNullsSorted') }}.MARG_HL_PROD_INT_MARGIN_CAT_ID,
		{{ ref('XfmCheckNulls__OutCheckNullsSorted') }}.MARG_MARGIN_TYPE,
		{{ ref('XfmCheckNulls__OutCheckNullsSorted') }}.MARG_MARGIN_DESC,
		{{ ref('XfmCheckNulls__OutCheckNullsSorted') }}.MARG_MARGIN_CODE,
		{{ ref('XfmCheckNulls__OutCheckNullsSorted') }}.MARG_MARGIN_AMOUNT,
		{{ ref('XfmCheckNulls__OutCheckNullsSorted') }}.MARG_ADJ_AMT,
		{{ ref('XfmCheckNulls__OutCheckNullsSorted') }}.RATE_FOUND_FLAG,
		{{ ref('XfmCheckNulls__OutCheckNullsSorted') }}.PERC_FOUND_FLAG,
		{{ ref('XfmCheckNulls__OutCheckNullsSorted') }}.MARG_FOUND_FLAG,
		{{ ref('CpyRejtRejectOra') }}.HL_INT_RATE_ID AS HL_INT_RATE_ID_R,
		{{ ref('CpyRejtRejectOra') }}.RATE_HL_INT_RATE_ID_R,
		{{ ref('CpyRejtRejectOra') }}.RATE_HL_APP_PROD_ID_R,
		{{ ref('CpyRejtRejectOra') }}.RATE_CASS_INT_RATE_TYPE_CODE_R,
		{{ ref('CpyRejtRejectOra') }}.RATE_RATE_SEQUENCE_R,
		{{ ref('CpyRejtRejectOra') }}.RATE_DURATION_R,
		{{ ref('CpyRejtRejectOra') }}.PERC_HL_INT_RATE_ID_R,
		{{ ref('CpyRejtRejectOra') }}.PERC_SCHEDULE_RATE_TYPE_R,
		{{ ref('CpyRejtRejectOra') }}.PERC_RATE_PERCENT_1_R,
		{{ ref('CpyRejtRejectOra') }}.PERC_RATE_PERCENT_2_R,
		{{ ref('CpyRejtRejectOra') }}.MARG_HL_PROD_INT_MARGIN_ID_R,
		{{ ref('CpyRejtRejectOra') }}.MARG_HL_INT_RATE_ID_R,
		{{ ref('CpyRejtRejectOra') }}.MARG_HL_PROD_INT_MARGIN_CAT_ID_R,
		{{ ref('CpyRejtRejectOra') }}.MARG_MARGIN_TYPE_R,
		{{ ref('CpyRejtRejectOra') }}.MARG_MARGIN_DESC_R,
		{{ ref('CpyRejtRejectOra') }}.MARG_MARGIN_CODE_R,
		{{ ref('CpyRejtRejectOra') }}.MARG_MARGIN_AMOUNT_R,
		{{ ref('CpyRejtRejectOra') }}.MARG_ADJ_AMT_R,
		{{ ref('CpyRejtRejectOra') }}.RATE_FOUND_FLAG_R,
		{{ ref('CpyRejtRejectOra') }}.PERC_FOUND_FLAG_R,
		{{ ref('CpyRejtRejectOra') }}.MARG_FOUND_FLAG_R,
		{{ ref('CpyRejtRejectOra') }}.ORIG_ETL_D_R
	FROM {{ ref('XfmCheckNulls__OutCheckNullsSorted') }}
	OUTER JOIN {{ ref('CpyRejtRejectOra') }} ON {{ ref('XfmCheckNulls__OutCheckNullsSorted') }}.HL_INT_RATE_ID = {{ ref('CpyRejtRejectOra') }}.HL_INT_RATE_ID
)

SELECT * FROM JoinSrcSortReject