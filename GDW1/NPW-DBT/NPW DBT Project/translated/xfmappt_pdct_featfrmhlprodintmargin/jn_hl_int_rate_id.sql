{{ config(materialized='view', tags=['XfmAPPT_PDCT_FEATFrmHlProdIntMargin']) }}

WITH JN_HL_INT_RATE_ID AS (
	SELECT
		{{ ref('DetermineTargetFoundFlag') }}.TGT_TBL_NAME,
		{{ ref('DetermineTargetFoundFlag') }}.SRC_TBL_NAME,
		{{ ref('DetermineTargetFoundFlag') }}.HL_INT_RATE_ID,
		{{ ref('DetermineTargetFoundFlag') }}.TGT_FOUND_FLAG,
		{{ ref('SrcRejectOra') }}.RATE_FOUND_FLAG,
		{{ ref('SrcRejectOra') }}.PERC_FOUND_FLAG,
		{{ ref('SrcRejectOra') }}.REJT_FOUND_FLAG
	FROM {{ ref('DetermineTargetFoundFlag') }}
	LEFT JOIN {{ ref('SrcRejectOra') }} ON {{ ref('DetermineTargetFoundFlag') }}.HL_INT_RATE_ID = {{ ref('SrcRejectOra') }}.HL_INT_RATE_ID
)

SELECT * FROM JN_HL_INT_RATE_ID