{{ config(materialized='view', tags=['ExtChlBusFeeBusFeeDisc']) }}

WITH JoinSrcSortReject AS (
	SELECT
		{{ ref('XfmCheckIdNull__OutXfmCheckIdNull') }}.HL_FEE_ID,
		{{ ref('XfmCheckIdNull__OutXfmCheckIdNull') }}.BF_HL_FEE_ID,
		{{ ref('XfmCheckIdNull__OutXfmCheckIdNull') }}.BF_HL_APP_PROD_ID,
		{{ ref('XfmCheckIdNull__OutXfmCheckIdNull') }}.BF_XML_CODE,
		{{ ref('XfmCheckIdNull__OutXfmCheckIdNull') }}.BF_DISPLAY_NAME,
		{{ ref('XfmCheckIdNull__OutXfmCheckIdNull') }}.BF_CATEGORY,
		{{ ref('XfmCheckIdNull__OutXfmCheckIdNull') }}.BF_UNIT_AMOUNT,
		{{ ref('XfmCheckIdNull__OutXfmCheckIdNull') }}.BF_TOTAL_AMOUNT,
		{{ ref('XfmCheckIdNull__OutXfmCheckIdNull') }}.BFD_HL_FEE_DISCOUNT_ID,
		{{ ref('XfmCheckIdNull__OutXfmCheckIdNull') }}.BFD_HL_FEE_ID,
		{{ ref('XfmCheckIdNull__OutXfmCheckIdNull') }}.BFD_DISCOUNT_REASON,
		{{ ref('XfmCheckIdNull__OutXfmCheckIdNull') }}.BFD_DISCOUNT_CODE,
		{{ ref('XfmCheckIdNull__OutXfmCheckIdNull') }}.BFD_DISCOUNT_AMT,
		{{ ref('XfmCheckIdNull__OutXfmCheckIdNull') }}.BFD_DISCOUNT_TERM,
		{{ ref('XfmCheckIdNull__OutXfmCheckIdNull') }}.BFD_HL_FEE_DISCOUNT_CAT_ID,
		{{ ref('XfmCheckIdNull__OutXfmCheckIdNull') }}.BF_FOUND_FLAG,
		{{ ref('XfmCheckIdNull__OutXfmCheckIdNull') }}.BFD_FOUND_FLAG,
		{{ ref('CpyRejectOra') }}.HL_FEE_ID AS HL_FEE_ID_R,
		{{ ref('CpyRejectOra') }}.BF_HL_FEE_ID_R,
		{{ ref('CpyRejectOra') }}.BF_HL_APP_PROD_ID_R,
		{{ ref('CpyRejectOra') }}.BF_XML_CODE_R,
		{{ ref('CpyRejectOra') }}.BF_DISPLAY_NAME_R,
		{{ ref('CpyRejectOra') }}.BF_CATEGORY_R,
		{{ ref('CpyRejectOra') }}.BF_UNIT_AMOUNT_R,
		{{ ref('CpyRejectOra') }}.BF_TOTAL_AMOUNT_R,
		{{ ref('CpyRejectOra') }}.BFD_HL_FEE_DISCOUNT_ID_R,
		{{ ref('CpyRejectOra') }}.BFD_HL_FEE_ID_R,
		{{ ref('CpyRejectOra') }}.BFD_DISCOUNT_REASON_R,
		{{ ref('CpyRejectOra') }}.BFD_DISCOUNT_CODE_R,
		{{ ref('CpyRejectOra') }}.BFD_DISCOUNT_AMT_R,
		{{ ref('CpyRejectOra') }}.BFD_DISCOUNT_TERM_R,
		{{ ref('CpyRejectOra') }}.BFD_HL_FEE_DISCOUNT_CAT_ID_R,
		{{ ref('CpyRejectOra') }}.BF_FOUND_FLAG_R,
		{{ ref('CpyRejectOra') }}.BFD_FOUND_FLAG_R,
		{{ ref('CpyRejectOra') }}.ORIG_ETL_D_R
	FROM {{ ref('XfmCheckIdNull__OutXfmCheckIdNull') }}
	OUTER JOIN {{ ref('CpyRejectOra') }} ON {{ ref('XfmCheckIdNull__OutXfmCheckIdNull') }}.HL_FEE_ID = {{ ref('CpyRejectOra') }}.HL_FEE_ID
)

SELECT * FROM JoinSrcSortReject