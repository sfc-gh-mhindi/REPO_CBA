{{ config(materialized='view', tags=['XfmAPPT_PDCT_FEATFrmPlFeePlMargin']) }}

WITH JN_PL_FEE_ID AS (
	SELECT
		{{ ref('DetermineTargetFoundFlag') }}.TGT_TBL_NAME,
		{{ ref('DetermineTargetFoundFlag') }}.SRC_TBL_NAME,
		{{ ref('DetermineTargetFoundFlag') }}.PL_FEE_ID,
		{{ ref('DetermineTargetFoundFlag') }}.PL_MARGIN_ID,
		{{ ref('DetermineTargetFoundFlag') }}.APPT_PDCT_I,
		{{ ref('DetermineTargetFoundFlag') }}.TGT_FOUND_FLAG,
		{{ ref('SrcRejectOra') }}.PL_FEE_FOUND_FLAG,
		{{ ref('SrcRejectOra') }}.REJT_FOUND_FLAG
	FROM {{ ref('DetermineTargetFoundFlag') }}
	LEFT JOIN {{ ref('SrcRejectOra') }} ON {{ ref('DetermineTargetFoundFlag') }}.PL_FEE_ID = {{ ref('SrcRejectOra') }}.PL_FEE_ID
)

SELECT * FROM JN_PL_FEE_ID