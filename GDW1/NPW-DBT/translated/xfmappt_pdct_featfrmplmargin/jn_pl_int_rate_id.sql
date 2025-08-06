{{ config(materialized='view', tags=['XfmAPPT_PDCT_FEATFrmPlMargin']) }}

WITH JN_PL_INT_RATE_ID AS (
	SELECT
		{{ ref('DetermineTargetFoundFlag') }}.TGT_TBL_NAME,
		{{ ref('DetermineTargetFoundFlag') }}.SRC_TBL_NAME,
		{{ ref('DetermineTargetFoundFlag') }}.PL_INT_RATE_ID,
		{{ ref('DetermineTargetFoundFlag') }}.PL_MARGIN_ID,
		{{ ref('DetermineTargetFoundFlag') }}.TGT_FOUND_FLAG,
		{{ ref('SrcRejectOra') }}.PL_INT_RATE_FOUND_FLAG,
		{{ ref('SrcRejectOra') }}.PL_INT_RATE_AMT_FOUND_FLAG,
		{{ ref('SrcRejectOra') }}.REJT_FOUND_FLAG
	FROM {{ ref('DetermineTargetFoundFlag') }}
	LEFT JOIN {{ ref('SrcRejectOra') }} ON {{ ref('DetermineTargetFoundFlag') }}.PL_INT_RATE_ID = {{ ref('SrcRejectOra') }}.PL_INT_RATE_ID
)

SELECT * FROM JN_PL_INT_RATE_ID