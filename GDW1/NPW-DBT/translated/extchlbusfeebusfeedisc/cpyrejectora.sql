{{ config(materialized='view', tags=['ExtChlBusFeeBusFeeDisc']) }}

WITH CpyRejectOra AS (
	SELECT
		HL_FEE_ID,
		{{ ref('SrcRejectOra') }}.BF_HL_FEE_ID AS BF_HL_FEE_ID_R,
		{{ ref('SrcRejectOra') }}.BF_HL_APP_PROD_ID AS BF_HL_APP_PROD_ID_R,
		{{ ref('SrcRejectOra') }}.BF_XML_CODE AS BF_XML_CODE_R,
		{{ ref('SrcRejectOra') }}.BF_DISPLAY_NAME AS BF_DISPLAY_NAME_R,
		{{ ref('SrcRejectOra') }}.BF_CATEGORY AS BF_CATEGORY_R,
		{{ ref('SrcRejectOra') }}.BF_UNIT_AMOUNT AS BF_UNIT_AMOUNT_R,
		{{ ref('SrcRejectOra') }}.BF_TOTAL_AMOUNT AS BF_TOTAL_AMOUNT_R,
		{{ ref('SrcRejectOra') }}.BFD_HL_FEE_DISCOUNT_ID AS BFD_HL_FEE_DISCOUNT_ID_R,
		{{ ref('SrcRejectOra') }}.BFD_HL_FEE_ID AS BFD_HL_FEE_ID_R,
		{{ ref('SrcRejectOra') }}.BFD_DISCOUNT_REASON AS BFD_DISCOUNT_REASON_R,
		{{ ref('SrcRejectOra') }}.BFD_DISCOUNT_CODE AS BFD_DISCOUNT_CODE_R,
		{{ ref('SrcRejectOra') }}.BFD_DISCOUNT_AMT AS BFD_DISCOUNT_AMT_R,
		{{ ref('SrcRejectOra') }}.BFD_DISCOUNT_TERM AS BFD_DISCOUNT_TERM_R,
		{{ ref('SrcRejectOra') }}.BFD_HL_FEE_DISCOUNT_CAT_ID AS BFD_HL_FEE_DISCOUNT_CAT_ID_R,
		{{ ref('SrcRejectOra') }}.BF_FOUND_FLAG AS BF_FOUND_FLAG_R,
		{{ ref('SrcRejectOra') }}.BFD_FOUND_FLAG AS BFD_FOUND_FLAG_R,
		{{ ref('SrcRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R
	FROM {{ ref('SrcRejectOra') }}
)

SELECT * FROM CpyRejectOra